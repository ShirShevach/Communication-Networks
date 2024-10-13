#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <math.h>

#define PORT 8080

int main(int argc, char const *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <Server IP Address>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *ip_address = argv[1];
    int msg_seq = 0;

    int sock = 0;
    struct sockaddr_in serv_addr;
    struct timespec start, end;

    // Create socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IP address from text to binary form
    if (inet_pton(AF_INET, ip_address, &serv_addr.sin_addr) <= 0)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    // Connect to the server
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }

    int num_messages = 1000000;
    int num_warm_messages = 10;

    for (int i = 0; i <= 15; ++i)
    {
        int BUFFER_SIZE = pow(2, i); // Change buffer size to a larger value
        // Create a buffer filled with data to send using malloc
        char *buffer = (char *)malloc(BUFFER_SIZE * sizeof(char));
        if (buffer == NULL) {
            perror("malloc");
            return 1;
        }

        // Fill buffer with 'A's or any other data
        memset(buffer, 'A', BUFFER_SIZE);

        // Send num_warm_messages to the server
        for (int j = 0; j < num_warm_messages; ++j) {
            if (send(sock, &buffer, BUFFER_SIZE, 0) != BUFFER_SIZE) {
                printf("Failed to send warm message %d\n", j+1);
                close(sock);
                return -1;
            }
        }

        // Record start time
        clock_gettime(CLOCK_MONOTONIC, &start);

        // Send num_messages to the server
        for (int j = 0; j < num_messages; ++j)
        {
            if (send(sock, &buffer, BUFFER_SIZE, 0) != BUFFER_SIZE)
            {
                printf("Failed to send message %d\n", j + 1);
                close(sock);
                return -1;
            }
        }

        // Record end time
        clock_gettime(CLOCK_MONOTONIC, &end);

        // Calculate elapsed time
        double elapsed_time = (end.tv_sec - start.tv_sec) +
                              (end.tv_nsec - start.tv_nsec) / 1e9;

        printf("Sent %d messages of %d bytes each to the server in %.6f seconds\n", num_messages, BUFFER_SIZE, elapsed_time);
        printf("%d\t%.6f\tseconds\n", BUFFER_SIZE, elapsed_time / num_messages);
        num_messages = num_messages / 2;
        free(buffer);
    }

    // Close the socket
    close(sock);
    return 0;
}
