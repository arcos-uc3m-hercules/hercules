#include "utils.h"

int get_hostname(char *buffer, size_t buffer_size) {
    // Check for a valid buffer and size.
    if (buffer == NULL || buffer_size == 0) {
        fprintf(stderr, "Error: Invalid buffer or buffer size provided.\n");
        return -1;
    }

    // Call the gethostname function.
    if (gethostname(buffer, buffer_size) == 0) 
    {
        buffer[buffer_size - 1] = '\0';
        return 0;
    } else {
        fprintf(stderr, "Error getting hostname: %s\n", strerror(errno));
        return -1;
    }
}

bool double_are_equal(double a, double b){
    return fabs(a - b) < epsilon;
}

