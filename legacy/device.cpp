#include <iostream>
#include <string>
#include <random>
#include <thread>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <atomic>
#include <mutex>
#include <cerrno>
#include <cstring>
#include <csignal>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>


struct DeviceData
{
    std::string deviceId;
    double temperature;
    double pressure;
    int rpm;
    std::string status;
};


const int PORT = 8080;


// ======================================================
// Device state
// ======================================================

std::atomic<bool> running(true);

// Normal = 2 seconds
// Slow down = 5 seconds
std::atomic<int> sendIntervalSeconds(2);

std::mutex coutMutex;


// ======================================================
// Reliable send
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
// Receive control commands from server
// ======================================================

void receiveCommands(
    int deviceSocket,
    const std::string& deviceId
)
{
    char buffer[512];

    std::string pendingData;


    while (running)
    {
        ssize_t bytesReceived = recv(
            deviceSocket,
            buffer,
            sizeof(buffer),
            0
        );


        if (bytesReceived < 0)
        {
            if (running)
            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cerr
                    << "[CONTROL ERROR] recv() failed: "
                    << std::strerror(errno)
                    << std::endl;
            }

            running = false;

            break;
        }


        if (bytesReceived == 0)
        {
            if (running)
            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "[CONTROL] Monitoring server disconnected."
                    << std::endl;
            }


            running = false;

            break;
        }


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
            std::string command =
                pendingData.substr(
                    0,
                    newlinePosition
                );


            pendingData.erase(
                0,
                newlinePosition + 1
            );


            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "\n[CONTROL] "
                    << deviceId
                    << " received command: "
                    << command
                    << std::endl;
            }


            // ==========================================
            // Execute control command
            // ==========================================

            if (command == "SLOW_DOWN")
            {
                sendIntervalSeconds = 5;


                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "[DEVICE] Transmission slowed to every 5 seconds."
                    << std::endl;
            }


            else if (command == "RESUME_NORMAL")
            {
                sendIntervalSeconds = 2;


                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "[DEVICE] Normal transmission restored to every 2 seconds."
                    << std::endl;
            }


            else if (command == "SHUTDOWN")
            {
                {
                    std::lock_guard<std::mutex> lock(coutMutex);

                    std::cout
                        << "[EMERGENCY] SHUTDOWN command received!"
                        << std::endl;

                    std::cout
                        << "[DEVICE] "
                        << deviceId
                        << " shutting down..."
                        << std::endl;
                }


                running = false;

                break;
            }


            else
            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cout
                    << "[DEVICE] Unknown control command."
                    << std::endl;
            }
        }
    }
}


// ======================================================
// Main
// ======================================================

int main(
    int argc,
    char* argv[]
)
{
    std::signal(
        SIGPIPE,
        SIG_IGN
    );


    // --------------------------------------------------
    // Device ID
    // --------------------------------------------------

    if (argc != 2)
    {
        std::cerr
            << "Usage: ./device <DEVICE_ID>"
            << std::endl;

        std::cerr
            << "Example: ./device MACHINE_01"
            << std::endl;

        return 1;
    }


    std::string deviceId =
        argv[1];


    // --------------------------------------------------
    // Create TCP socket
    // --------------------------------------------------

    int deviceSocket =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (deviceSocket < 0)
    {
        std::cerr
            << "Error creating device socket."
            << std::endl;

        return 1;
    }


    // --------------------------------------------------
    // Server address
    // --------------------------------------------------

    sockaddr_in serverAddress{};


    serverAddress.sin_family =
        AF_INET;


    serverAddress.sin_port =
        htons(PORT);


    if (
        inet_pton(
            AF_INET,
            "127.0.0.1",
            &serverAddress.sin_addr
        ) <= 0
    )
    {
        std::cerr
            << "Invalid server address."
            << std::endl;


        close(deviceSocket);

        return 1;
    }


    // --------------------------------------------------
    // Connect
    // --------------------------------------------------

    std::cout
        << "Connecting to monitoring server..."
        << std::endl;


    if (
        connect(
            deviceSocket,
            reinterpret_cast<sockaddr*>(
                &serverAddress
            ),
            sizeof(serverAddress)
        ) < 0
    )
    {
        std::cerr
            << "Error connecting to server: "
            << std::strerror(errno)
            << std::endl;


        close(deviceSocket);

        return 1;
    }


    std::cout
        << "Connected to monitoring server!"
        << std::endl;


    // --------------------------------------------------
    // Start control-command receiver thread
    // --------------------------------------------------

    std::thread controlThread(
        receiveCommands,
        deviceSocket,
        deviceId
    );


    // --------------------------------------------------
    // Sensor generators
    // --------------------------------------------------

    std::random_device rd;

    std::mt19937 generator(
        rd()
    );


    std::uniform_real_distribution<double>
        temperatureGenerator(
            40.0,
            100.0
        );


    std::uniform_real_distribution<double>
        pressureGenerator(
            20.0,
            50.0
        );


    std::uniform_int_distribution<int>
        rpmGenerator(
            1000,
            4000
        );


    {
        std::lock_guard<std::mutex> lock(coutMutex);

        std::cout
            << "\n========================================\n";

        std::cout
            << " Equipment Monitoring Device Simulator\n";

        std::cout
            << "========================================\n";

        std::cout
            << "Device started: "
            << deviceId
            << "\n\n";
    }


    // --------------------------------------------------
    // Generate + transmit readings
    // --------------------------------------------------

    while (running)
    {
        DeviceData data;


        data.deviceId =
            deviceId;


        data.temperature =
            temperatureGenerator(
                generator
            );


        data.pressure =
            pressureGenerator(
                generator
            );


        data.rpm =
            rpmGenerator(
                generator
            );


        // Keep local warning logic consistent
        // with central monitoring rules.
        if (
            data.temperature > 90.0
            ||
            data.pressure > 45.0
            ||
            data.rpm > 3500
        )
        {
            data.status =
                "WARNING";
        }
        else
        {
            data.status =
                "RUNNING";
        }


        // ------------------------------------------------
        // Display local sensor reading
        // ------------------------------------------------

        {
            std::lock_guard<std::mutex> lock(coutMutex);


            std::cout
                << std::fixed
                << std::setprecision(2);


            std::cout
                << "["
                << data.deviceId
                << "] "
                << "Temperature: "
                << data.temperature
                << " C | Pressure: "
                << data.pressure
                << " PSI | RPM: "
                << data.rpm
                << " | Status: "
                << data.status
                << std::endl;
        }


        // ------------------------------------------------
        // Serialize
        // ------------------------------------------------

        std::ostringstream messageStream;


        messageStream
            << std::fixed
            << std::setprecision(2)
            << data.deviceId
            << ","
            << data.temperature
            << ","
            << data.pressure
            << ","
            << data.rpm
            << ","
            << data.status
            << "\n";


        std::string message =
            messageStream.str();


        // ------------------------------------------------
        // Send sensor data to server
        // ------------------------------------------------

        if (!sendAll(
                deviceSocket,
                message
            ))
        {
            {
                std::lock_guard<std::mutex> lock(coutMutex);

                std::cerr
                    << "[ERROR] Failed to send sensor data."
                    << std::endl;
            }


            running = false;

            break;
        }


        // ------------------------------------------------
        // Dynamic transmission speed
        // ------------------------------------------------

        int waitTime =
            sendIntervalSeconds.load();


        std::this_thread::sleep_for(
            std::chrono::seconds(
                waitTime
            )
        );
    }


    // --------------------------------------------------
    // Shutdown
    // --------------------------------------------------

    shutdown(
        deviceSocket,
        SHUT_RDWR
    );


    if (controlThread.joinable())
    {
        controlThread.join();
    }


    close(deviceSocket);


    std::cout
        << "Device stopped: "
        << deviceId
        << std::endl;


    return 0;
}