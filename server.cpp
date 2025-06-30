#include <iostream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <string>
#include <map>
#include <memory>
#include <sstream>
#include <iomanip>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <chrono>
#include <unordered_map>
#include <set>
#include <atomic>
#include <fstream>
#include <ctime>
#include <sstream>
// God fucking help me if I need any more libraries //
// I wonder how many of these actually end up getting used //

using namespace std;

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "libcrypto.lib")
#pragma comment(lib, "libssl.lib")

const int PORT = 3708;
const int RECV_BUFFER_SIZE = 4096; // Standard buffer size for receiving chunks

const string IMAGE_PREFIX = "IMAGE_DATA:";


const string FILE_PREFIX = "FILE_DATA:";

int MAX_FILE_MESSAGE_SIZE = 2000 * 1024; // 2000 KB (2MB)
string MAX_FILE_MESSAGE_STR = "MAX_FILE_SIZE:" + to_string(MAX_FILE_MESSAGE_SIZE);


// Shared resources protected by a mutex
vector<SOCKET> clients;
map<SOCKET, string> client_usernames;
mutex clients_mutex;

unordered_map<SOCKET, chrono::steady_clock::time_point> last_message_time;
const int MIN_SECONDS_BETWEEN_MESSAGES = 1; // 1 second cooldown

set<string> banned_usernames;
atomic<bool> server_running{true};

string CALC_SHA256(const string& input);
ofstream log_file;

SOCKET get_socket_by_username(const string& username);

void broadcast_message(const string& message, SOCKET sender_socket) {
    // Ensure the mutex is not locked before acquiring it
    {
        lock_guard<mutex> lock(clients_mutex);
        string hash = CALC_SHA256(message);
        string message_with_hash = message + "|" + hash + "\n"; // Append newline delimiter
        for (SOCKET client_socket : clients) {
            if (client_socket != sender_socket) {
                send(client_socket, message_with_hash.c_str(), static_cast<int>(message_with_hash.length()), 0);
            }
        }
    } 
    // Mutex is released here automatically... at least, I hope it is... //
}

void relay_raw_packet(const string& packet, SOCKET sender_socket) {
    // Again, ensure the mutex is not locked before acquiring it
    {
        lock_guard<mutex> lock(clients_mutex);
        string packet_with_newline = packet + "\n"; // Ensure newline delimiter is present or else shit breaks. //
        for (SOCKET client_sock : clients) {
            if (client_sock != sender_socket) {
                send(client_sock, packet_with_newline.c_str(), static_cast<int>(packet_with_newline.length()), 0);
            }
        }
    }
}

void handle_client(SOCKET client_socket, const string& client_ip) {
    {
        lock_guard<mutex> lock(clients_mutex);
        clients.push_back(client_socket);
        client_usernames[client_socket] = "Anonymous";
        last_message_time[client_socket] = chrono::steady_clock::now() - chrono::seconds(MIN_SECONDS_BETWEEN_MESSAGES);
    }

    // Send a welcome message to the newly connected client
    string welcome_msg = "Welcome to the server, " + client_ip + "!";
    string welcome_msg_with_hash = welcome_msg + "|" + CALC_SHA256(welcome_msg) + "\n";
    send(client_socket, welcome_msg_with_hash.c_str(), static_cast<int>(welcome_msg_with_hash.length()), 0);
    send(client_socket, MAX_FILE_MESSAGE_STR.c_str(), static_cast<int>(MAX_FILE_MESSAGE_STR.length()), 0);

    auto recv_buffer = make_unique<char[]>(RECV_BUFFER_SIZE);
    string accumulated_data;
    int bytes_received = 0;

    try {
        // Main receive loop with accumulation buffer
        while ((bytes_received = recv(client_socket, recv_buffer.get(), RECV_BUFFER_SIZE, 0)) > 0) {
            accumulated_data.append(recv_buffer.get(), bytes_received);

            size_t pos;
            while ((pos = accumulated_data.find('\n')) != string::npos) {
                string message = accumulated_data.substr(0, pos);
                accumulated_data.erase(0, pos + 1); // Erase the processed message and the '\n'

                auto now = chrono::steady_clock::now();
                {
                    lock_guard<mutex> lock(clients_mutex);
                    auto last = last_message_time[client_socket];
                    auto elapsed = chrono::duration_cast<chrono::seconds>(now - last).count();
                    if (elapsed < MIN_SECONDS_BETWEEN_MESSAGES) {
                        string warn = "Server: Please wait before sending another message.\n"; // Warn the client against doing dumb shit //
                        send(client_socket, warn.c_str(), static_cast<int>(warn.length()), 0);
                        continue; // Don't even bother with the message //
                    }
                    last_message_time[client_socket] = now;
                }

                if (message.rfind(IMAGE_PREFIX, 0) == 0) {
                    string sender_username;
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        sender_username = client_usernames[client_socket];
                    }
                    cout << "[" << sender_username << " from " << client_ip << "] Relaying image data." << endl;
                    // Relay the raw image packet as-is to other clients //
                    relay_raw_packet(message, client_socket);

                } 
                else if (message.rfind(FILE_PREFIX, 0) == 0) {
                    string sender_username;
                    {
                        lock_guard<mutex> lock(clients_mutex);
                        sender_username = client_usernames[client_socket];
                    }
                    cout << sender_username << " sent a file." << endl;
                    relay_raw_packet(message, client_socket);
                }

                else {
                    size_t sep = message.rfind('|');
                    if (sep != string::npos && sep < message.length() - 1) {
                        string msg_part = message.substr(0, sep);
                        string hash_part = message.substr(sep + 1);

                        // Handle nickname changes
                        if (msg_part.rfind("/nick ", 0) == 0) {
                            string new_username = msg_part.substr(6);
                            if (banned_usernames.count(new_username)) {
                                string msg = "Server: This username is banned.\n";
                                send(client_socket, msg.c_str(), static_cast<int>(msg.length()), 0);
                                closesocket(client_socket);
                                {
                                    lock_guard<mutex> lock(clients_mutex);
                                    clients.erase(remove(clients.begin(), clients.end(), client_socket), clients.end());
                                    client_usernames.erase(client_socket);
                                    last_message_time.erase(client_socket);
                                }
                                return;
                            }
                            string old_username;
                            {
                                lock_guard<mutex> lock(clients_mutex);
                                old_username = client_usernames[client_socket];
                                client_usernames[client_socket] = new_username;
                            }
                            string status_msg = "Server: " + old_username + " is now known as " + new_username;
                            cout << status_msg << endl;
                            broadcast_message(status_msg, client_socket);
                        }
                        else if (msg_part.rfind("/dm ", 0) == 0 || msg_part.rfind("/pm ", 0) == 0) {
                            // Format: /dm username message
                            istringstream iss(msg_part);
                            string cmd, target_username, dm_message;
                            iss >> cmd >> target_username;
                            getline(iss, dm_message);
                            dm_message = dm_message.substr(1); // Remove leading space

                            SOCKET target_socket = get_socket_by_username(target_username);
                            if (target_socket != INVALID_SOCKET) {
                                string sender_username;
                                {
                                    lock_guard<mutex> lock(clients_mutex);
                                    sender_username = client_usernames[client_socket];
                                }
                                string dm_text = "[DM from " + sender_username + "]: " + dm_message;
                                string dm_with_hash = dm_text + "|" + CALC_SHA256(dm_text) + "\n";
                                send(target_socket, dm_with_hash.c_str(), static_cast<int>(dm_with_hash.length()), 0);

                                // Optional: notify sender that DM was sent
                                string confirm = "[DM to " + target_username + "]: " + dm_message;
                                string confirm_with_hash = confirm + "|" + CALC_SHA256(confirm) + "\n";
                                send(client_socket, confirm_with_hash.c_str(), static_cast<int>(confirm_with_hash.length()), 0);
                            } else {
                                string err = "Server: User '" + target_username + "' not found.\n";
                                send(client_socket, err.c_str(), static_cast<int>(err.length()), 0);
                            }
                        }
                        // Handle regular chat messages
                        else {
                            if (CALC_SHA256(msg_part) == hash_part) {
                                time_t t = time(nullptr);
                                tm tm;
                                if (localtime_s(&tm, &t) != 0) {
                                    cerr << "Failed to convert time to local time." << endl;
                                    return;
                                }
                                ostringstream oss;
                                oss << put_time(&tm, "%Y-%m-%d %H:%M:%S");
                                string date = oss.str();

                                log_file.open("log.txt", ios::app);
                                string sender_username;
                                {
                                    lock_guard<mutex> lock(clients_mutex);
                                    sender_username = client_usernames[client_socket];
                                }
                                string message_to_broadcast = sender_username + " says: " + msg_part;

                                log_file << date << " " << message_to_broadcast << endl; // Write to a log file so people can expose embarrassing shit that's said. //
                                log_file.close();

                                cout << "Broadcasting: " << message_to_broadcast << endl;
                                broadcast_message(message_to_broadcast, client_socket);
                            } else {
                                cerr << "Corrupted message from " << client_ip << ": Hash Mismatch!" << endl;
                            }
                        }
                    } else {
                        // This shouldn't happen. Like at all. //
                        cerr << "Malformed message from " << client_ip << ": No hash separator '|'." << endl;
                    }
                }
            }
        }
    } catch (...) {
        cerr << "An exception occurred while handling client " << client_ip << endl;
    }

    // Handle client disconnection
    string disconnected_username;
    {
        lock_guard<mutex> lock(clients_mutex);
        disconnected_username = client_usernames[client_socket];
        // Remove client from lists //
        clients.erase(remove(clients.begin(), clients.end(), client_socket), clients.end());
        client_usernames.erase(client_socket);
        last_message_time.erase(client_socket);
    }

    if (bytes_received == 0) {
        string disconnect_msg = disconnected_username + " (" + client_ip + ") disconnected.";
        cout << disconnect_msg << endl;
        broadcast_message(disconnect_msg, INVALID_SOCKET); // Broadcast to all remaining clients
    } else {
        string error_msg = "recv failed with error " + to_string(WSAGetLastError()) + " for client " + client_ip;
        cerr << error_msg << endl;
        string disconnect_msg = disconnected_username + " (" + client_ip + ") disconnected due to an error.";
        broadcast_message(disconnect_msg, INVALID_SOCKET);
    }

    closesocket(client_socket);
}

// Function to calculate SHA256 hash using OpenSSL EVP API //
string CALC_SHA256(const string& input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);

    if (!ctx) {
        throw runtime_error("Failed to create EVP_MD_CTX");
    }

    if (EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(ctx.get(), input.c_str(), input.length()) != 1 ||
        EVP_DigestFinal_ex(ctx.get(), hash, nullptr) != 1) {
        throw runtime_error("Failed to compute SHA256 hash");
    }

    stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << hex << setw(2) << setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
}

// Console command thread, so server hosts can enter console commands locally. //
// It's also here because I cannot be fucked figuring out how to do client permissions properly. //
void console_command_thread() {
    string line;
    while (server_running) {
        getline(cin, line);
        if (line.rfind("/ban ", 0) == 0) {
            string username = line.substr(5);
            {
                lock_guard<mutex> lock(clients_mutex);
                banned_usernames.insert(username);
                // Disconnect all clients with this username. Hope to god nobody needs to ban "Anonymous" because everyone will be banned. TwT //
                for (auto it = client_usernames.begin(); it != client_usernames.end(); ++it) {
                    if (it->second == username) {
                        SOCKET sock = it->first;
                        string msg = "Server: You have been banned.\n";
                        send(sock, msg.c_str(), static_cast<int>(msg.length()), 0);
                        closesocket(sock);
                        clients.erase(remove(clients.begin(), clients.end(), sock), clients.end());
                        last_message_time.erase(sock);
                    }
                }
            }
            cout << "User '" << username << "' has been banned." << endl;
        } else if (line.rfind("/unban ", 0) == 0) {
            string username = line.substr(7);
            lock_guard<mutex> lock(clients_mutex);
            banned_usernames.erase(username);
            cout << "User '" << username << "' has been unbanned." << endl;
        }
        else if (line.rfind("/maxfilesize ") == 0) {
            int newFileSize;
            istringstream(line.substr(9)) >> newFileSize;
            MAX_FILE_MESSAGE_SIZE = newFileSize * 1024;

            string fileMessage = "MAX_FILE_SIZE:" + MAX_FILE_MESSAGE_SIZE;

        }
    }
}

bool username_exists(const string& username) {
    lock_guard<mutex> lock(clients_mutex);
    for (const auto& pair : client_usernames) {
        if (pair.second == username) {
            return true;
        }
    }
    return false;
}

SOCKET get_socket_by_username(const string& username) {
    lock_guard<mutex> lock(clients_mutex);
    for (const auto& pair : client_usernames) {
        if (pair.second == username) {
            return pair.first;
        }
    }
    return INVALID_SOCKET;
}

int main() {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        // Pretty sure this shouldn't happen, but it might. //
        cerr << "Failed to initialize Winsock: " << WSAGetLastError() << endl;
        return 1;
    }

    SOCKET server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == INVALID_SOCKET) {
        // Couldn't bind to the port that we wanted. Suck shit. //
        cerr << "Could not create socket: " << WSAGetLastError() << endl;
        WSACleanup();
        return 1;
    }

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) == SOCKET_ERROR) {
        // Couldn't bind to the IP that we wanted. Sucked in. //
        cerr << "Bind failed: " << WSAGetLastError() << endl;
        closesocket(server_fd);
        WSACleanup();
        return 1;
    }

    if (listen(server_fd, SOMAXCONN) == SOCKET_ERROR) {
        cerr << "Listen failed: " << WSAGetLastError() << endl;
        closesocket(server_fd);
        WSACleanup();
        return 1;
    }

    cout << "Server listening on port " << PORT << ". Ready for connections." << endl;

    thread(console_command_thread).detach();

    while (true) {
        sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);
        SOCKET new_socket = accept(server_fd, (struct sockaddr*)&client_addr, &client_addr_len);
        if (new_socket == INVALID_SOCKET) {
            // I genuinely do not know how this could happen but if something breaks, it's here. //
            cerr << "Accept failed: " << WSAGetLastError() << endl;
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);

        cout << "New connection from " << client_ip << endl;

        thread client_thread(handle_client, new_socket, string(client_ip));
        client_thread.detach();
    }

    closesocket(server_fd);
    WSACleanup();
    return 0;
}