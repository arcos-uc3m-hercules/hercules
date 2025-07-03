#include <imss.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <directory.h>
#include <stdio.h>

// Pointer to the tree's root node.
GNode *tree_root;
GNode *last_parent = NULL;
pthread_mutex_t tree_mut = PTHREAD_MUTEX_INITIALIZER;

// Helper function to print the tree structure
void print_tree_structure(GNode *root, int depth)
{
	if (!root)
		return;

	for (int i = 0; i < depth; i++)
	{
		fprintf(stdout, "  "); // Indent
	}
	fprintf(stdout, "- %s\n", (const char *)root->data);

	// Recursively print children
	GNode *child = root->children;
	while (child)
	{
		print_tree_structure(child, depth + 1);
		child = child->next;
	}
}

// Method searching for a certain data node.
int32_t GTree_search_(GNode *parent_node,
					  const char *desired_data,
					  GNode **found_node)
{
	// slog_debug("Looking at %s", parent_node->data);
	size_t len_desired_data = strlen(desired_data);
	const int desired_is_directory = (desired_data[len_desired_data - 1] == '/');

	// Number of children of the current parent_node.
	uint32_t num_children = g_node_n_children(parent_node);
	GNode *child = parent_node->children;

	*found_node = parent_node;
	// fprintf(stdout, "TreeSearch number of child %ld in %p, %s\n", num_children, parent_node, (char *)parent_node->data);
	slog_debug("TreeSearch number of child %ld in %p, %s\n", num_children, parent_node, (char *)parent_node->data);
	//  Search for the requested data within the children of the current node.
	for (int32_t i = 0; i < num_children; i++)
	// while (child != NULL)
	{
		const char *child_data_str = (const char *)child->data;
		size_t len_child_data = strlen((char *)child->data);
		// Search for a directory antecesor of the desired node.
		// HAVE TO CHECK IF IT IS A DIRECTORY OR A FILE
		// For this i check if it has at the end /
		// slog_debug("child->data=%s, desired_data=%s", child->data, desired_data);
		if (desired_is_directory && !strncmp(child_data_str, desired_data, len_child_data))
		{ // desired is directory.
			// slog_debug("directory case, child=%s, desired_data=%s", child_data_str, desired_data);
			// Check if the compared node is the requested one.
			if (!strcmp(child_data_str, desired_data))
			{
				*found_node = child;
				slog_debug("data was found after %d iterations", i);
				// The desired data was found.
				return 1;
			}
			else
			{
				// Check within the new node.
				return GTree_search_(child, desired_data, found_node);
			}
		}
		else if (!desired_is_directory && !strncmp(child_data_str, desired_data, len_child_data))
		{ // desired is regular file.
			//			// slog_debug("regular file");
			// slog_debug("regular file case, child=%s, desired_data=%s", child_data_str, desired_data);
			// Check if the compared node is the requested one.
			if (!strcmp(child_data_str, desired_data))
			{
				*found_node = child;
				slog_debug("data was found after %d iterations", i);
				// fprintf(stdout,"found node %p, %s, desird data %s\n", child, (char *)child->data, desired_data);
				//  The desired data was found.
				return 1;
			}
			else
			{
				// SPECIAL CASE EXAMPLE PRUEBA_1 CREATED AND WANT TO ADD PRUEBA_11
				// CHECK THE NUMBERS OF '/' IN THE PATHS TO SEE IF WE ARRIVE TO THE DIRECTORY
				// int amount = 0;
				// for (int32_t j = 0; j < len_desired_data - 1; j++)
				// { // counts the number of "/" on the path.
				// 	if (desired_data[j] == '/')
				// 	{
				// 		amount = amount + 1;
				// 	}
				// }
				// int amount_child = 0;
				// char *path_child = (char *)child->data;
				// //				// slog_debug("path_child=%s, desired_data=%s", path_child, desired_data);
				// for (int32_t j = 0; j < len_child_data - 1; j++)
				// { // counts the number of "/" on the child path.
				// 	if (path_child[j] == '/')
				// 	{
				// 		amount_child = amount_child + 1;
				// 	}
				// }
				// //				// slog_debug("amount=%d, amount_child=%d", amount, amount_child);
				// if (amount == amount_child)
				// {
				// 	// Move on to the following child.
				// 	child = child->next;
				// 	continue;
				// }

				if (len_child_data < len_desired_data &&
					child_data_str[len_child_data - 1] == '/' && // Child must be a directory
					strncmp(child_data_str, desired_data, len_child_data) == 0)
				{
					// Check within the new node.
					// slog_debug("Going inside the directory %s", child->data);
					if (GTree_search_(child, desired_data, found_node))
					{
						return 1;
					}
				}
			}
		}

		// Move on to the following child.
		// slog_debug("Moving to the next child");
		child = child->next;
	}
	last_parent = parent_node;
	// slog_debug("last parent=%s", last_parent->data);
	return 0;
}

// Wrapper to the GTree_search_ function that compares if the parent node is requested.
int32_t GTree_search(GNode *parent_node,
					 const char *desired_data,
					 GNode **found_node)
{
	// fprintf(stdout,"Searching for %s\n, parent node %s\n", desired_data, (char *)parent_node->data);
	// slog_debug("Searching for %s, parent node %s", desired_data, (char *)parent_node->data);
	//  Check if the desired data was contained by the provided node.
	if (!strcmp((char *)parent_node->data, desired_data))
	{
		*found_node = parent_node;

		return 1;
	}
	// slog_debug("Init searching of %s", desired_data);
	pthread_mutex_lock(&tree_mut);
	int32_t ret = TIMING(GTree_search_(parent_node, desired_data, found_node), "Rercursive search", int32_t, 0);
	pthread_mutex_unlock(&tree_mut);
	return ret;
}

// Method renaming a new path.
int32_t GTree_rename(char *old_desired_data, char *new_desired_data)
{
	// int ret = -1; // to error handling.
	// Closest node to the one requested (or even the requested one itself).
	GNode *closest_node;

	// Check if the node has been already inserted.
	slog_debug("old_desired_data=%s, new_desired_data=%s", old_desired_data, new_desired_data);
	// pthread_mutex_lock(&tree_mut);
	int32_t ret = GTree_search(tree_root, old_desired_data, &closest_node);
	// slog_debug("ret from GTree_search = %d", ret);
	// pthread_mutex_unlock(&tree_mut);
	if (ret == 1)
	{
		// slog_debug("\t[GTree] closest_node->data=%s", (char *)closest_node->data);
		// If the searched name (old data) and the data of the node in the tree are equals,
		// we remove the node from the tree, and insert the new one.
		if (strcmp(old_desired_data, (char *)closest_node->data) == 0)
		{
			pthread_mutex_lock(&tree_mut);
			free(closest_node->data);
			g_node_destroy(closest_node);
			pthread_mutex_unlock(&tree_mut);
			GNode *new_node;
			ret = GTree_insert(new_desired_data, &new_node);
			if (ret == -1)
			{
				return -1;
			}
		}
	}
	else
	{
		// //fprintf(stderr, "Rename Error not found:%s\n", old_desired_data);
		slog_error("Rename Error not found:%s", old_desired_data);
		return -1;
	}

	return 1;
}

// --- Helper function to print the tree structure for verification ---
void print_tree_recursive(GNode *node, int indent)
{
	if (!node)
		return;

	// for (int i = 0; i < indent; i++) {
	//     slog_debug("  ");
	// }
	slog_debug("%s", (char *)node->data);

	GNode *child = g_node_first_child(node);
	while (child)
	{
		print_tree_recursive(child, indent + 1);
		child = g_node_next_sibling(child);
	}
}

// --- Helper function to update a node's path ---
static void update_node_path(GNode *node, const char *old_prefix, const char *new_prefix)
{
	char *current_path = (char *)node->data;
	if (!current_path)
	{
		slog_debug("Warning: Node data is NULL, skipping path update for node %p.", (void *)node);
		return;
	}

	size_t old_prefix_len = strlen(old_prefix);
	char new_full_path[PATH_MAX];

	// Check if current_path actually starts with old_prefix
	if (strncmp(current_path, old_prefix, old_prefix_len) == 0)
	{
		// Now, we must ensure it's a "directory prefix" match.
		// If old_prefix is "/a/b/" and current_path is "/a/bc/", this is not a match.
		// We need to check the character right after the old_prefix.
		// It must be either '/' (if it's a subdirectory) or '\0' (if it's the directory itself).
		// Since old_prefix is guaranteed to end with a '/', we just check if
		// current_path's remainder is what follows it.
		if (current_path[old_prefix_len - 1] == '/' || current_path[old_prefix_len] == '\0')
		{
			// Correctly calculate the suffix by skipping the old_prefix part
			const char *suffix = current_path + old_prefix_len;

			strcpy(new_full_path, new_prefix);
			// Ensure new_prefix (rdir_dest) ends with a slash if it's a directory
			ConcatLastSlashC(new_full_path); // Ensure new_prefix has trailing slash
			strcat(new_full_path, suffix);	 // Append the rest of the path (the suffix)

			slog_debug("Changing path from '%s' to '%s'", current_path, new_full_path);

			g_free(current_path);				  // Free the old g_strdup'd string
			node->data = g_strdup(new_full_path); // Assign the new g_strdup'd string
		}
		else
		{
			// This case occurs for paths like "/home/user-temp/" when old_prefix is "/home/user/"
			// This is not a direct child path of old_prefix, so skip.
			slog_debug("Path '%s' does not represent a child or self of old prefix '%s'. Skipping update.", current_path, old_prefix);
			return;
		}
	}
	else
	{
		// Path doesn't start with the old prefix at all.
		slog_debug("Path '%s' does not start with old prefix '%s'. Skipping update.", current_path, old_prefix);
		return;
	}
}

// GNodeTraverseFunc for g_node_traverse for renaming
static gboolean rename_dir_traverse_func(GNode *node, gpointer data)
{
	char **prefixes = (char **)data;
	const char *old_dir_prefix = prefixes[0];
	const char *rdir_dest_prefix = prefixes[1];

	update_node_path(node, old_dir_prefix, rdir_dest_prefix);

	return FALSE; // Continue traversal (TRUE would stop traversal, but we want to rename all)
}

// Method renaming dir to dir.
int32_t GTree_rename_dir_dir(char *old_dir, char *rdir_dest)
{
	slog_debug("printing tree");
	print_tree_recursive(tree_root, 0);

	// Node whose elements must be retrieved.
	GNode *dir_node;
	// pthread_mutex_lock(&tree_mut);
	slog_debug("Renaming dir %s to %s", old_dir, rdir_dest);
	int32_t ret = GTree_search(tree_root, old_dir, &dir_node);
	// pthread_mutex_unlock(&tree_mut);
	// Check if the node has been already inserted.
	if (ret == 1)
	{ // node found.

		char *prefixes[2];
		prefixes[0] = old_dir;
		prefixes[1] = rdir_dest;

		g_node_traverse(dir_node, G_IN_ORDER, G_TRAVERSE_ALL, -1, rename_dir_traverse_func, prefixes);

		slog_debug("Successfully renamed dir %s to %s", old_dir, rdir_dest);

		// // Counts the total number of nodes in the entire subtree (starting in "dir_node" and excluding it) recursively.
		// uint32_t num_elements_indir = g_node_n_nodes(dir_node, G_TRAVERSE_ALL) - 1;
		// // Counts the number of direct children of "dir_node".
		// uint32_t num_elements_indir_childrens = g_node_n_children(dir_node);
		// // printf("DIR_NUM_ELEMENTS=%d\n",num_elements_indir+1);
		// slog_debug("Num of sub-children=%d, Num of direct-children=%d", num_elements_indir, num_elements_indir_childrens);
		// char *dir_elements_buff = (char *)calloc((num_elements_indir + 1), URI_);
		// if (dir_elements_buff == NULL)
		// {
		// 	perror("HERCULES_ERR_GTREE_RENAME_DIR_DIR_MEMORY_ALLOC");
		// 	slog_fatal("HERCULES_ERR_GTREE_RENAME_DIR_DIR_MEMORY_ALLOC");
		// 	exit(-1);
		// }

		// char *aux_dir_elem = dir_elements_buff;
		// serialize_dir(dir_node, num_elements_indir_childrens, &aux_dir_elem);

		// // The first element is the old_dir.
		// char *aux = dir_elements_buff;
		// size_t len = 0;
		// // Changes the prefix (directory name) of all elements in the old_dir to the new_dir.
		// for (int i = 0; i < num_elements_indir + 1; i++)
		// {
		// 	if (strstr(aux, old_dir) != NULL)
		// 	{
		// 		char *path = aux;
		// 		len = strlen(old_dir);
		// 		if (len > 0)
		// 		{
		// 			char *p = path;
		// 			while ((p = strstr(p, old_dir)) != NULL)
		// 			{
		// 				memmove(p, p + len, strlen(p + len) + 1);
		// 			}
		// 		}
		// 		char *new_path = (char *)malloc(PATH_MAX);
		// 		strcpy(new_path, rdir_dest);
		// 		if (strlen(path) > 0)
		// 		{
		// 			// check if the rdir_dest does not have the last slash (it is a directory, must contain it).
		// 			ConcatLastSlashC(new_path);
		// 			strcat(new_path, path);
		// 		}
		// 		slog_debug("new_path to be inserted=%s", new_path);
		// 		GTree_insert(new_path);
		// 		free(new_path);
		// 	}
		// 	aux += URI_;
		// }
		// if (dir_elements_buff != NULL)
		// 	free(dir_elements_buff);

		// if (dir_node->data != NULL)
		// 	free(dir_node->data);

		// pthread_mutex_lock(&tree_mut);
		// g_node_destroy(dir_node);
		// pthread_mutex_unlock(&tree_mut);
	}
	else
	{ //  error case.
		slog_debug("Old dir %s does not exists.", old_dir);
		return -1;
	}
	print_tree_recursive(tree_root, 0);

	return 0;
}

/**
 * @brief Method deleting an entry of the tree.
 * @return On success, 1 is returned, 0 on error.
 * */
int32_t GTree_delete(std::string desired_data)
{
	// Closest node to the one requested (or even the requested one itself).
	GNode *closest_node;
	int32_t ret = 0;

	// Check if the node has been already inserted.
	// pthread_mutex_lock(&tree_mut);
	ret = GTree_search(tree_root, desired_data.c_str(), &closest_node);
	// pthread_mutex_unlock(&tree_mut);
	if (ret == 1)
	{
		if (strcmp(desired_data.c_str(), (char *)closest_node->data) == 0)
		{
			// fprintf(stdout,"deleting address %p, %s, desired data %s\n", closest_node, (char *)closest_node->data, desired_data);
			pthread_mutex_lock(&tree_mut);
			free(closest_node->data);
			g_node_destroy(closest_node); // Delete Node
			pthread_mutex_unlock(&tree_mut);
			ret = 1;
		}
	}
	// else
	// {
	// 	// add the last slash to search as directory.
	// 	int added = ConcatLastSlash(desired_data);
	// 	if (added == 1)
	// 		ret = GTree_delete(desired_data);
	// }

	return ret;
}

// Method inserting a new path.
int32_t GTree_insert(char *desired_data, GNode **new_node)
{
	// Closest node to the one requested (or even the requested one itself).
	GNode *closest_node = NULL;
	// slog_debug("is last parent null? %d", last_parent == NULL);
	size_t len_desired_data = strlen(desired_data);
	if (last_parent != NULL)
	{
		const char *last_parent_data = (const char *)last_parent->data;
		// // slog_debug("last parent=%s, len_desired_data=%s", last_parent_data, len_desired_data);
		const char *last_slash_pos = NULL;
		if (len_desired_data > 0)
		{
			// If desired_data ends with a slash, we need to consider the parent of that.
			// If it's just "/", then the parent is effectively empty or root.
			if (desired_data[len_desired_data - 1] == '/')
			{
				if (len_desired_data == 1)
				{
					last_slash_pos = NULL; // Indicate no parent path beyond root
				}
				else
				{
					size_t temp_len = len_desired_data - 1; // Exclude the last slash
					while (temp_len > 0 && desired_data[temp_len - 1] != '/')
					{ // find the len up to remaining last slash.
						temp_len--;
					}
					if (temp_len > 0 && desired_data[temp_len - 1] == '/')
					{
						last_slash_pos = desired_data + (temp_len - 1);
					}
					else
					{
						last_slash_pos = NULL; // No leading slash found for parent, or it's the root itself
					}
				}
			}
			else
			{
				// search for the last slash directly
				// Devuelve un puntero a la última aparición de '/' en serie. Si no se encuentra el carácter especificado, se devuelve un puntero NULL.
				last_slash_pos = strrchr(desired_data, '/');
			}
		}

		// Determine the length of the "father" path
		size_t len_father_path = 0;
		if (last_slash_pos != NULL)
		{
			// The father path is from the beginning of desired_data up to and including the last slash.
			// slog_debug("last_slash_pos=%s", last_slash_pos);
			len_father_path = (last_slash_pos - desired_data) + 1;
		}
		else
		{
			// If no slash found, the father path is effectively empty or root.
			len_father_path = 0; // Represents the root itself or no parent path.
		}

		// slog_debug("len_father_path=%d, last_parent_data=%s", len_father_path, last_parent_data);

		if (strlen(last_parent_data) == len_father_path && strncmp(last_parent_data, desired_data, len_father_path) == 0)
		{
			// return current_last_parent;
			closest_node = last_parent;
			// slog_debug("re-using last parent as closest node=%s", closest_node->data);
		}

		// // // slog_debug("last_parent->data=%s, desired_data=%s", last_parent->data, desired_data);
		// char *data_search = (char *)calloc(URI_, sizeof(char));
		// if (desired_data[strlen(desired_data) - 1] == '/')
		// { // skip last slash.
		// 	memcpy(data_search, desired_data, strlen(desired_data) - 1);
		// }
		// else
		// {
		// 	memcpy(data_search, desired_data, strlen(desired_data));
		// }
		// char *father = (char *)calloc(URI_, sizeof(char));
		// // Devuelve un puntero a la última aparición de '/' en serie. Si no se encuentra el carácter especificado, se devuelve un puntero NULL.
		// char *lastson = strrchr(data_search, '/');
		// int copy = (strlen(data_search) - strlen(lastson));

		// memcpy(father, &data_search[0], copy + 1);
		//		slog_live("desired_data=%s, data_search=%s, lastson=%s, father=%s", desired_data, data_search, lastson, father);
		// Compares the data on the current node (last_parent) against the Hercules instance (e.g., imss://Makefile and imss://).
		// if (strncmp((char *)last_parent->data, father, strlen((char *)father)) == 0 && strlen((char *)last_parent->data) == strlen(father))
		// {
		// 	closest_node = last_parent;
		// }
		// free(father);
		// free(data_search);
	}

	// Check if the node has been already inserted.
	if (closest_node == NULL)
	{
		// pthread_mutex_lock(&tree_mut);
		// slog_debug("Looking for the closest_node of %s", desired_data);
		int32_t ret = TIMING(GTree_search(tree_root, desired_data, &closest_node), "**GTree_search", int32_t, 0);
		// pthread_mutex_unlock(&tree_mut);
		if (ret)
		{
			//			// slog_debug("closest_node=%s", closest_node->data);
			slog_debug("%s already inserted on the tree, add=%p", closest_node->data, closest_node);
			*new_node = closest_node;
			return 0;
		}
	}

	// Length of the found uri. An additional unit is added in order to avoid the first '/' encountered.
	// int32_t closest_data_length = strlen((char *)closest_node->data);
	// // int32_t closest_data_length = strlen((char *)closest_node->data) + 1;

	// // Number of characters that the desired string has more than the found one.
	// int32_t more_chars = strlen(desired_data) - closest_data_length;

	// // slog_debug("closest_data_length=%d, more_chars=%d", closest_data_length, more_chars);

	// // Special case: insertion of a one character length file in the root directory.
	// if (!more_chars && (closest_data_length == 2))
	// {
	// 	more_chars = 1;
	// 	closest_data_length--;
	// }

	// Search for the '/' characters within the additional ones.
	//	// slog_debug("path=%s, more_chars=%d, closest_data_length=%d", desired_data, more_chars, closest_data_length);
	// for (int32_t i = 0; i < more_chars; i++)
	// {
	// 	int32_t new_position = closest_data_length + i;
	// 	// slog_debug("[Gtree] path=%s, new_position=%d, i=%d, %c", desired_data, new_position, i, desired_data[new_position]);

	// 	if ((desired_data[new_position] == '/') || (i == (more_chars - 1)))
	// 	{
	// 		if (i == (more_chars - 1))
	// 			new_position++;

	// 		// if (i == 0 && desired_data[new_position+1] == '/')

	// 		// String that will be introduced as a new node.
	// 		// char *new_data = (char *)malloc(new_position + 1);
	char *new_data = (char *)calloc(len_desired_data + 1, sizeof(char)); // (char *)malloc(len_desired_data + 1);
	strncpy(new_data, desired_data, len_desired_data);
	// 		// New node to be introduced.
	// 		// printf("new_node=%s\n",new_data);
	// GNode *
	*new_node = g_node_new((void *)new_data);

	// Introduce it as a child of the closest one found.
	// // slog_debug("[GTree] inserting in the tree=%s", new_data);
	// fprintf(stdout,"Inserting node %p, %s on  %p, %s\n", new_node, (char *)new_node->data, closest_node, (char *)closest_node->data);
	pthread_mutex_lock(&tree_mut);
	// slog_debug("Appending %s in %s", new_node->data, closest_node->data);
	TIMING_NO_RETURN(g_node_append(closest_node, *new_node), "\t\tg_node_append", 0);
	slog_debug("new node add during insert=%p", *new_node);
	// print_tree_structure(tree_root, 0);
	pthread_mutex_unlock(&tree_mut);

	return 0;
}

// Method serializing the number of childrens within a directory into a buffer.
// int32_t serialize_dir_childrens(GNode *visited_node, uint32_t num_children, char **buffer)
int32_t serialize_dir_childrens(GNode *visited_node, uint32_t num_children, char **buffer, std::shared_ptr<map_records> map)
{
	// Add the concerned uri into the buffer.
	// memcpy(*buffer, (char *)visited_node->data, URI_);
	// *buffer += URI_;

	GNode *child = visited_node->children;
	int found = 0;
	int num_dirty_child = 0;
	// printf("node=%s  num_children=%d\n",(char *) visited_node->data,num_children);
	for (int32_t i = 0; i < num_children; i++)
	{
		// Number of children of the current child node.

		// uint32_t num_grandchildren = g_node_n_children(child);

		// If the child is a leaf one, just store the corresponding info.
		/*if (!num_grandchildren)
		{*/
		if (child->data == NULL)
		{
			perror("HERCULES_ERR_SERIALIZE_DIR_CHILDRENS_CHILD_DATA");
			slog_fatal("HERCULES_ERR_SERIALIZE_DIR_CHILDRENS_CHILD_DATA");
			exit(-1);
		}

		// Check if the child data is dirty.
		found = map->garbage_collector_search(std::string((char *)(child->data)));
		if (!found)
		{
			memcpy(*buffer, (char *)child->data, strlen((char *)child->data));
			*buffer += URI_;
		}
		else
		{
			num_dirty_child++;
		}

		child = child->next;
	}

	return num_dirty_child;
}

// Method serializing the number of elements within a directory into a buffer.
int32_t
serialize_dir(GNode *visited_node,
			  uint32_t num_nodes,
			  char **buffer)
{
	// Add the concerned uri into the buffer.
	memset(*buffer, 0, URI_);
	memcpy(*buffer, (char *)visited_node->data, strlen((char *)visited_node->data));
	*buffer += URI_;

	GNode *child = visited_node->children;
	// printf("node=%s  num_nodes=%d\n",(char *) visited_node->data,num_nodes);
	slog_debug("node=%s  num_nodes=%d", (char *)visited_node->data, num_nodes);
	for (int32_t i = 0; i < num_nodes; i++)
	{
		// Number of children of the current child node.
		uint32_t num_grandchildren = g_node_n_children(child);

		// If the child is a leaf one, just store the corresponding info.
		if (!num_grandchildren)
		{
			// Add the child's uri to the buffer.
			memset(*buffer, 0, URI_);
			memcpy(*buffer, (char *)child->data, strlen((char *)child->data));
			*buffer += URI_;
		}
		else
		{
			serialize_dir(child, num_grandchildren, buffer);
		}
		child = child->next;
	}

	return 0;
}

/**********************************************************/
// WARNING: this function reserves memory that must be freed.
/**********************************************************/

void print_child_node(GNode *node, gpointer data)
{
	// Cast the node's data back to a const char*
	const char *node_name = (const char *)node->data;

	fprintf(stdout, "  Child Node: %s\n", node_name);
}

// Method retrieving a buffer with all the files within a directory.
// extern std::shared_ptr<map_records> g_map;
char *GTree_getdir(char *desired_dir, int32_t *numdir_elems, std::shared_ptr<map_records> map)
{
	// Node whose elements must be retrieved.
	GNode *dir_node;

	// print_tree_structure(tree_root, 0);

	// Check if the node is inserted.
	// pthread_mutex_lock(&tree_mut);
	int32_t ret = GTree_search(tree_root, desired_dir, &dir_node);
	// pthread_mutex_unlock(&tree_mut);
	if (!ret)
	{
		// fprintf(stdout,"Number of files in node %p, %s: %d\n", dir_node, (char * )dir_node->data, -1);
		*numdir_elems = -1;
		return NULL;
	}

	// Number of elements contained by the concerned directory.
	// uint32_t num_elements_indir = g_node_n_nodes (dir_node, G_TRAVERSE_ALL);

	//*numdir_elems = num_elements_indir;

	// Number of children of the directory node.
	pthread_mutex_lock(&tree_mut);
	uint32_t num_children = g_node_n_children(dir_node);
	pthread_mutex_unlock(&tree_mut);
	// fprintf(stdout,"Number of files in node %p, %s: %d\n", dir_node, (char * )dir_node->data, num_children);
	// g_node_children_foreach(dir_node, G_TRAVERSE_ALL, print_child_node, NULL);
	// *numdir_elems = num_children + 1; //+1 because of the actual directory + childrens
	*numdir_elems = num_children; // actual directory is concat in the front-end.

	// slog_info("num_children=%d", *numdir_elems);

	if (*numdir_elems == 0)
	{
		return NULL;
	}

	// Buffer containing the whole set of elements within a certain directory.
	// char *dir_elements_buff = (char *) malloc(sizeof(char)*num_elements_indir*URI_);
	// char *dir_elements_buff = (char *)calloc(1, (num_children + 1) * URI_);
	char *dir_elements_buff = (char *)calloc(num_children + 1, URI_);
	if (dir_elements_buff == NULL)
	{
		perror("HERCULES_ERR_GTREE_GETDIR_ALLOC_MEMORY");
		slog_fatal("HERCULES_ERR_GTREE_GETDIR_ALLOC_MEMORY");
		exit(-1);
	}

	char *aux_dir_elem = dir_elements_buff;
	int num_dirty_child = 0;

	// Call the serialization function storing all dir elements in the buffer.
	// TO CHECK!
	//	slog_info("serialize_dir_childrens(dir_node=%s, num_children=%d, &aux_dir_elem)", dir_node->data, num_children);
	pthread_mutex_lock(&tree_mut);
	num_dirty_child = serialize_dir_childrens(dir_node, num_children, &aux_dir_elem, map);
	pthread_mutex_unlock(&tree_mut);
	// slog_info("ending serialize_dir_childrens, aux_dir_elem=%s", *aux_dir_elem);
	slog_debug("directory %s has %d elements but %d are dirty", desired_dir, *numdir_elems, num_dirty_child);
	*numdir_elems -= num_dirty_child;
	slog_debug("valid numdir_elems=%d", *numdir_elems);

	return dir_elements_buff;
}

// Method that will be called for each tree node freeing the associated data element.
int32_t
gnodetraverse(GNode *node,
			  void *data)
{
	free(node->data);

	return 0;
}
