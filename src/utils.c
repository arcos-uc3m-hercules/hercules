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

int is_valid_integer(const char *str, int *out_val)
{
    char *endptr = NULL;
    errno = 0;
    
    // convert the string to a long integer using base 10.
    // endptr will point the address of the first invalid character if it exists.
    long val = strtol(str, &endptr, 10);

    // checks for limits.
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0)) {
        return 0;
    }

    // "If there were no digits at all, strtol() stores the original value of str in *endptr (and returns 0)".
    if (endptr == str) {
        return 0;
    }

    // check for a valid end of line.
    if (*endptr != '\0' && *endptr != '\n') {
        return 0;
    }

    // the string is a valid integer number, cast it to int.
    *out_val = (int)val;
    return 1;
}


int is_valid_long(const char *str, long *out_val)
{
    if (str == NULL || *str == '\0') {
        return 0;
    }

    char *endptr = NULL;
    errno = 0;
    
    // strtol checks for limits.
    long val = strtol(str, &endptr, 10);

    // Check for overflow or underflow as reported by errno
    if (errno == ERANGE) {
        return 0;
    }

    // Verify that at least one digit was processed
    if (endptr == str) {
        return 0;
    }

    // Ensure the string ends correctly
    if (*endptr != '\0' && *endptr != '\n') {
        return 0;
    }

    *out_val = val;
    return 1;
}

int is_valid_double(const char *str, double *out_val)
{
    if (str == NULL || *str == '\0') {
        return 0;
    }

    char *endptr = NULL;
    errno = 0;
    
    double val = strtod(str, &endptr);

    // Check for overflow or underflow as reported by errno
    if (errno == ERANGE) {
        return 0;
    }

    // Verify that at least one valid character was processed
    if (endptr == str) {
        return 0;
    }

    // Ensure the string ends correctly
    if (*endptr != '\0' && *endptr != '\n') {
        return 0;
    }

    *out_val = val;
    return 1;
}