#ifndef UTILS_
#define UTILS_

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h> // for fabs in double comparition.
#include <stdbool.h> // for bool.
#include <limits.h>
#include <stdlib.h> // for strtol.
#include <ctype.h>
#include <stdint.h>

// Definiciones de colores ANSI
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"


#define STR(x) #x
#define XSTR(x) STR(x)

int get_hostname(char *buffer, size_t buffer_size);

/**
 * @brief Calculates the difference between two doubles in order to 
 * compare if they are almost equals.
 * @return true if the values are under the tolerance value, false in other case. 
 */
static const double epsilon = 1e-9; // A small tolerance value
bool double_are_equal(double a, double b);

/**
 * @brief Checks if a string is a valid integer number.
 * 
 * @param str Input string to be checked.
 * @param out_val int pointer where the value will be stored if the "str" is valid.
 * @return int 1 if the string is a valid integer number, 0 on other case.
 */
int is_valid_integer(const char *str, int *out_val);

/**
 * @brief Checks if a string is a valid uint32_t number.
 * 
 * @param str Input string to be checked.
 * @param out_val uint32_t pointer where the value will be stored if the "str" is valid.
 * @return int 1 if the string is a valid uint32_t, 0 on other case.
 */
int is_valid_uint32(const char *str, uint32_t *out_val);

/**
 * @brief Checks if a string is a valid long integer number.
 * 
 * @param str Input string to be checked.
 * @param out_val long pointer where the value will be stored if the "str" is valid.
 * @return int 1 if the string is a valid long number, 0 on other case.
 */
int is_valid_long(const char *str, long *out_val);

/**
 * @brief Checks if a string is a valid double diuble number.
 * 
 * @param str Input string to be checked.
 * @param out_val double pointer where the value will be stored if the "str" is valid.
 * @return int 1 if the string is a valid double number, 0 on other case.
 */
int is_valid_double(const char *str, double *out_val);


#endif
