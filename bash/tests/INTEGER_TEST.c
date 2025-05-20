#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

int main(int argc, char **argv)
{
    // Declare variables
    int number = (int) -1594983473; //1024 * 1024 * 1024 * 2; //  2G (2147483648)
    int number_read = -1;

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <mount_point>\n", argv[0]);
        return EXIT_FAILURE;
    }

    printf("Number: %d\n", number);

    char *mount_point = argv[1];
    char file_path[100] = {'\0'};

    strncpy(file_path, mount_point, strlen(mount_point));
    strcat(file_path, "number.txt");

    // Open file for writing, create it if it doesn't exist, truncate it to zero length
    int fileDescriptor = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

    // Check if the file is successfully opened
    if (fileDescriptor == -1)
    {
        perror("Error opening file");
        return 1; // Exit with an error code
    }

    // Write the number to the file
    // dprintf(fileDescriptor, "%d", number);
    write(fileDescriptor, &number, sizeof(number));

    // Close the file
    close(fileDescriptor);

    printf("Number has been written to the file: %s\n", file_path);

    // Open file for reading
    fileDescriptor = open(file_path, O_RDONLY);

    // Check if the file is successfully opened
    if (fileDescriptor == -1)
    {
        perror("Error opening file");
        return 1; // Exit with an error code
    }

    // Read the number from the file
    int bytesRead = read(fileDescriptor, &number_read, sizeof(number));

    // Close the file
    close(fileDescriptor);

    if (bytesRead == -1)
    {
        perror("Error reading from file");
        return 1; // Exit with an error code
    }

    printf("Number read from the file: %d\n", number_read);

    if (number != number_read)
    {
        printf("Error: Numbers are differents!\n");
    } else {
        printf("Ok :) Numbers are equals.\n");
    }
    

    return 0; // Exit successfully
}
