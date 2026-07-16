#ifndef FCNTL_H
#define FCNTL_H

#include <dlfcn.h>
#include <fcntl.h>
#include <stddef.h>

enum FcntlArgType
{
	FCNTL_ARG_NONE,
	FCNTL_ARG_INT,
	FCNTL_ARG_PTR
};

/**
 * @brief Helper to translate command integers back to their macro literals
 *
 */
static const char *fcntl_cmd_to_str(int cmd)
{
	switch (cmd)
	{
	case F_DUPFD:
		return "F_DUPFD";
	case F_DUPFD_CLOEXEC:
		return "F_DUPFD_CLOEXEC";
	case F_GETFD:
		return "F_GETFD";
	case F_SETFD:
		return "F_SETFD";
	case F_GETFL:
		return "F_GETFL";
	case F_SETFL:
		return "F_SETFL";
#ifdef F_GETLK
	case F_GETLK:
		return "F_GETLK";
#endif
#ifdef F_SETLK
	case F_SETLK:
		return "F_SETLK";
#endif
#ifdef F_SETLKW
	case F_SETLKW:
		return "F_SETLKW";
#endif
#ifdef F_GETOWN
	case F_GETOWN:
		return "F_GETOWN";
#endif
#ifdef F_SETOWN
	case F_SETOWN:
		return "F_SETOWN";
#endif
	default:
		return NULL;
	}
}

/**
 * @brief Categorize commands by their expected third argument type to ensure safe type-casting
 */
static FcntlArgType get_fcntl_arg_type(int cmd)
{
	switch (cmd)
	{
	case F_GETFD:
	case F_GETFL:
#ifdef F_GETOWN
	case F_GETOWN:
#endif
		return FCNTL_ARG_NONE;

	case F_DUPFD:
	case F_DUPFD_CLOEXEC:
	case F_SETFD:
	case F_SETFL:
#ifdef F_SETOWN
	case F_SETOWN:
#endif
		return FCNTL_ARG_INT;

	case F_GETLK:
	case F_SETLK:
	case F_SETLKW:
		return FCNTL_ARG_PTR;

	default:
		// Fallback to pointer for unknown or custom structure extensions
		return FCNTL_ARG_PTR;
	}
}

#endif