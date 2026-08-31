#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <sstream>
#include <fstream>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cerrno>
#include <cstring>
#include <unordered_map>
#include <csignal>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>


// ======================================================
// Configuration
// ======================================================

const int PORT = 8080;

const double WARNING_TEMPERATURE = 90.0;
const double WARNING_PRESSURE = 45.0;
const int WARNING_RPM = 3500;

// Critical thresholds are intentionally high so
// simulated machines do not shut down too frequently.
const double CRITICAL_TEMPERATURE = 99.5;
const double CRITICAL_PRESSURE = 49.8;


// ======================================================
// Global synchronization
// ======================================================

std::mutex coutMutex;
std::mutex logMutex;

std::mutex commandStateMutex;

std::unordered_map<int, std::string> lastCommandBySocket;


// ======================================================
// Logging
// ======================================================

std::string getCurrentTimestamp()
{
    auto now = std::chrono::system_clock::now();

    std::time_t currentTime =
        std::chrono::system_clock::to_time_t(now);

    std::tm timeInfo{};

    localtime_r(
        &currentTime,
        &timeInfo
    );

    std::ostringstream timestamp;

    timestamp
        << std::put_time(
            &timeInfo,
            "%Y-%m-%d %H:%M:%S"
        );

    return timestamp.str();
}


void logMessage(
    const std::string& level,
    const std::string& message
)
{
    std::lock_guard<std::mutex> lock(logMutex);

    std::ofstream logFile(
        "logs/equipment.log",
        std::ios::app
    );

    if (!logFile.is_open())
    {
        std::lock_guard<std::mutex> consoleLock(coutMutex);

        std::cerr
            << "[LOGGER ERROR] Unable to open log file."
            << std::endl;

        return;
    }

    logFile
        << getCurrentTimestamp()
        << " [" << level << "]"
        << " [Thread "
        << std::this_thread::get_id()
        << "] "
        << message
        << std::endl;
}


// ======================================================
// Equipment reading
// ======================================================

struct EquipmentReading
{
    std::string deviceId;
    double temperature;
    double pressure;
    int rpm;
    std::string status;
};


// ======================================================
// Queue event
//
// We now store the socket too because the worker needs
// to know WHICH device should receive a command.
// ======================================================

struct DeviceEvent
{
    int clientSocket;
    std::string clientIp;
    std::string message;
};


// ======================================================
// Thread-safe producer-consumer queue
// ======================================================

template <typename T>
class ThreadSafeQueue
{
private:

    std::queue<T> queue;
    std::mutex queueMutex;
    std::condition_variable condition;

public:

    void push(const T& value)
    {
        {
            std::lock_guard<std::mutex> lock(queueMutex);

            queue.push(value);
        }

        condition.notify_one();
    }


    T pop()
    {
        std::unique_lock<std::mutex> lock(queueMutex);

        condition.wait(
            lock,
            [this]()
            {
                return !queue.empty();
            }
        );

        T value = queue.front();

        queue.pop();

        return value;
    }
};


ThreadSafeQueue<DeviceEvent> messageQueue;


// ======================================================
// Parse device message
// ======================================================

bool parseMessage(
    const std::string& message,
    EquipmentReading& reading
)
{
    std::stringstream ss(message);

    std::string temperatureString;
    std::string pressureString;
    std::string rpmString;


    if (!std::getline(ss, reading.deviceId, ','))
    {
        return false;
    }

    if (!std::getline(ss, temperatureString, ','))
    {
        return false;
    }

    if (!std::getline(ss, pressureString, ','))
    {
        return false;
    }

    if (!std::getline(ss, rpmString, ','))
    {
        return false;
    }

    if (!std::getline(ss, reading.status))
    {
        return false;
    }


    try
    {
        reading.temperature =
            std::stod(temperatureString);

        reading.pressure =
            std::stod(pressureString);

        reading.rpm =
            std::stoi(rpmString);
    }
    catch (...)
    {
        return false;
    }


    return true;
}


// ======================================================
// Reliable TCP send
// ======================================================

bool sendAll(
    int socket,
    const std::string& data
)
{
    size_t totalSent = 0;

    while (totalSent < data.size())
    {
        ssize_t bytesSent = send(
            socket,
            data.c_str() + totalSent,
            data.size() - totalSent,
            0
        );

        if (bytesSent <= 0)
        {
            return false;
        }

        totalSent +=
            static_cast<size_t>(bytesSent);
    }

    return true;
}


// ======================================================
// Send command from server -> device
// ======================================================

void sendControlCommand(
    int clientSocket,
    const std::string& deviceId,
    const std::string& command
)
{
    // Avoid sending the exact same command repeatedly.
    {
        std::lock_guard<std::mutex> lock(commandStateMutex);

        auto it =
            lastCommandBySocket.find(clientSocket);

        if (
            it != lastCommandBySocket.end() &&
            it->second == command
        )
        {
            return;
        }

        lastCommandBySocket[clientSocket] =
            command;
    }


    std::string networkMessage =
        command + "\n";


    if (!sendAll(
            clientSocket,
            networkMessage
        ))
    {
        std::string errorMessage =
            "Failed to send command "
            + command
            + " to "
            + deviceId;

        logMessage(
            "ERROR",
            errorMessage
        );

        return;
    }


    logMessage(
        "CONTROL",
        deviceId
        + " COMMAND="
        + command
    );


    {
        std::lock_guard<std::mutex> lock(coutMutex);

        std::cout
            << "[CONTROL] "
            << deviceId
            << " <- "
            << command
            << std::endl;
    }
}


// ======================================================
// Consumer / monitoring worker
// ======================================================

void processMessages()
{
    while (true)
    {
        DeviceEvent event =
            messageQueue.pop();


        EquipmentReading reading;


        if (!parseMessage(
                event.message,
                reading
            ))
        {
            logMessage(
                "ERROR",
                "Invalid equipment message: "
                + event.message
            );

            continue;
        }


        // ------------------------------------------------
        // Log sensor reading
        // ------------------------------------------------

        std::ostringstream readingLog;

        readingLog
            << reading.deviceId
            << " TEMP="
            << std::fixed
            << std::setprecision(2)
            << reading.temperature
            << " PRESSURE="
            << reading.pressure
            << " RPM="
            << reading.rpm
            << " STATUS="
            << reading.status;


        logMessage(
            "INFO",
            readingLog.str()
        );


        // ------------------------------------------------
        // Monitor sensor values
        // ------------------------------------------------

        bool alertTriggered = false;

        std::ostringstream alertOutput;


        if (
            reading.temperature >
            WARNING_TEMPERATURE
        )
        {
            alertTriggered = true;

            alertOutput
                << "[ALERT] "
                << reading.deviceId
                << " -> HIGH TEMPERATURE: "
                << reading.temperature
                << " C\n";


            logMessage(
                "WARNING",
                reading.deviceId
                + " HIGH_TEMPERATURE="
                + std::to_string(
                    reading.temperature
                )
            );
        }


        if (
            reading.pressure >
            WARNING_PRESSURE
        )
        {
            alertTriggered = true;

            alertOutput
                << "[ALERT] "
                << reading.deviceId
                << " -> HIGH PRESSURE: "
                << reading.pressure
                << " PSI\n";


            logMessage(
                "WARNING",
                reading.deviceId
                + " HIGH_PRESSURE="
                + std::to_string(
                    reading.pressure
                )
            );
        }


        if (
            reading.rpm >
            WARNING_RPM
        )
        {
            alertTriggered = true;

            alertOutput
                << "[ALERT] "
                << reading.deviceId
                << " -> HIGH RPM: "
                << reading.rpm
                << "\n";


            logMessage(
                "WARNING",
                reading.deviceId
                + " HIGH_RPM="
                + std::to_string(
                    reading.rpm
                )
            );
        }


        // ------------------------------------------------
        // Print structured reading
        // ------------------------------------------------

        {
            std::lock_guard<std::mutex> lock(coutMutex);

            std::cout
                << "\n----------------------------------------\n";

            std::cout
                << "Device       : "
                << reading.deviceId
                << "\n";

            std::cout
                << "Temperature  : "
                << std::fixed
                << std::setprecision(2)
                << reading.temperature
                << " C\n";

            std::cout
                << "Pressure     : "
                << reading.pressure
                << " PSI\n";

            std::cout
                << "RPM          : "
                << reading.rpm
                << "\n";

            std::cout
                << "Device Status: "
                << reading.status
                << "\n";


            if (alertTriggered)
            {
                std::cout
                    << alertOutput.str();
            }
            else
            {
                std::cout
                    << "[OK] Equipment operating normally.\n";
            }


            std::cout
                << "----------------------------------------"
                << std::endl;
        }


        // =================================================
        // CENTRAL CONTROL LOGIC
        // =================================================

        std::string command;


        // Critical equipment condition
        if (
            reading.temperature >
                CRITICAL_TEMPERATURE
            ||
            reading.pressure >
                CRITICAL_PRESSURE
        )
        {
            command = "SHUTDOWN";
        }

        // Warning condition
        else if (
            reading.temperature >
                WARNING_TEMPERATURE
            ||
            reading.pressure >
                WARNING_PRESSURE
            ||
            reading.rpm >
                WARNING_RPM
        )
        {
            command = "SLOW_DOWN";
        }

        // Equipment recovered
        else
        {
            command = "RESUME_NORMAL";
        }


        sendControlCommand(
            event.clientSocket,
            reading.deviceId,
            command
        );
    }
}


// ======================================================
// Producer / network thread
// ======================================================

void handleClient(
    int clientSocket,
    std::string clientIp
)
{
    logMessage(
        "INFO",
        "Device connected from "
        + clientIp
        + " on socket "
        + std::to_string(clientSocket)
    );


    {
        std::lock_guard<std::mutex> lock(coutMutex);

        std::cout
            << "[Network Thread "
            << std::this_thread::get_id()
            << "] Device connected from "
            << clientIp
            << " (socket "
            << clientSocket
            << ")"
            << std::endl;
    }


    char buffer[1024];

    std::string pendingData;


    while (true)
    {
        ssize_t bytesReceived = recv(
            clientSocket,
            buffer,
            sizeof(buffer),
            0
        );


        if (bytesReceived < 0)
        {
            int errorNumber = errno;

            std::string errorMessage =
                "recv() failed on socket "
                + std::to_string(clientSocket)
                + ": "
                + std::strerror(errorNumber);


            logMessage(
                "ERROR",
                errorMessage
            );

            break;
        }


        if (bytesReceived == 0)
        {
            logMessage(
                "INFO",
                "Device "
                + clientIp
                + " disconnected from socket "
                + std::to_string(clientSocket)
            );


            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "[Network Thread "
                    << std::this_thread::get_id()
                    << "] Device disconnected."
                    << std::endl;
            }


            break;
        }


        // TCP may split or combine messages.
        pendingData.append(
            buffer,
            bytesReceived
        );


        size_t newlinePosition;


        while (
            (newlinePosition =
                pendingData.find('\n'))
            != std::string::npos
        )
        {
            std::string message =
                pendingData.substr(
                    0,
                    newlinePosition
                );


            pendingData.erase(
                0,
                newlinePosition + 1
            );


            DeviceEvent event;

            event.clientSocket =
                clientSocket;

            event.clientIp =
                clientIp;

            event.message =
                message;


            // PRODUCER
            messageQueue.push(event);
        }
    }


    {
        std::lock_guard<std::mutex> lock(commandStateMutex);

        lastCommandBySocket.erase(
            clientSocket
        );
    }


    close(clientSocket);
}


// ======================================================
// Main
// ======================================================

int main()
{
    // Prevent application termination if TCP send()
    // is attempted after a client disconnects.
    std::signal(
        SIGPIPE,
        SIG_IGN
    );


    // --------------------------------------------------
    // Start monitoring consumer
    // --------------------------------------------------

    std::thread workerThread(
        processMessages
    );

    workerThread.detach();


    // --------------------------------------------------
    // Create TCP socket
    // --------------------------------------------------

    int serverSocket =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (serverSocket < 0)
    {
        std::cerr
            << "Error creating server socket."
            << std::endl;

        return 1;
    }


    // --------------------------------------------------
    // Port reuse
    // --------------------------------------------------

    int option = 1;


    setsockopt(
        serverSocket,
        SOL_SOCKET,
        SO_REUSEADDR,
        &option,
        sizeof(option)
    );


    // --------------------------------------------------
    // Address
    // --------------------------------------------------

    sockaddr_in serverAddress{};


    serverAddress.sin_family =
        AF_INET;


    serverAddress.sin_port =
        htons(PORT);


    serverAddress.sin_addr.s_addr =
        INADDR_ANY;


    // --------------------------------------------------
    // Bind
    // --------------------------------------------------

    if (
        bind(
            serverSocket,
            reinterpret_cast<sockaddr*>(
                &serverAddress
            ),
            sizeof(serverAddress)
        ) < 0
    )
    {
        std::cerr
            << "Bind failed: "
            << std::strerror(errno)
            << std::endl;


        close(serverSocket);

        return 1;
    }


    // --------------------------------------------------
    // Listen
    // --------------------------------------------------

    if (
        listen(
            serverSocket,
            10
        ) < 0
    )
    {
        std::cerr
            << "Listen failed."
            << std::endl;


        close(serverSocket);

        return 1;
    }


    logMessage(
        "INFO",
        "Equipment monitoring server started on port "
        + std::to_string(PORT)
    );


    std::cout
        << "\n============================================\n";

    std::cout
        << " Multithreaded Equipment Monitoring Server\n";

    std::cout
        << "============================================\n";

    std::cout
        << "Producer-Consumer Queue : ACTIVE\n";

    std::cout
        << "Thread-Safe Logging     : ACTIVE\n";

    std::cout
        << "Centralized Monitoring  : ACTIVE\n";

    std::cout
        << "Two-Way Device Control  : ACTIVE\n";

    std::cout
        << "Listening on port "
        << PORT
        << "...\n";

    std::cout
        << "Waiting for devices...\n\n";


    // --------------------------------------------------
    // Accept clients
    // --------------------------------------------------

    while (true)
    {
        sockaddr_in clientAddress{};


        socklen_t clientAddressLength =
            sizeof(clientAddress);


        int clientSocket = accept(
            serverSocket,
            reinterpret_cast<sockaddr*>(
                &clientAddress
            ),
            &clientAddressLength
        );


        if (clientSocket < 0)
        {
            logMessage(
                "ERROR",
                std::string("accept() failed: ")
                + std::strerror(errno)
            );

            continue;
        }


        char clientIpBuffer[
            INET_ADDRSTRLEN
        ];


        const char* ipResult =
            inet_ntop(
                AF_INET,
                &clientAddress.sin_addr,
                clientIpBuffer,
                sizeof(clientIpBuffer)
            );


        std::string clientIp;


        if (ipResult != nullptr)
        {
            clientIp =
                clientIpBuffer;
        }
        else
        {
            clientIp =
                "UNKNOWN";
        }


        std::thread clientThread(
            handleClient,
            clientSocket,
            clientIp
        );


        clientThread.detach();
    }


    close(serverSocket);

    return 0;
}