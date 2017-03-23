/**************************************************************************************************
 * Authors:
 *   Zhiyuan Shao
 *
 * Declaration:
 *   Prototype mmapped files.
 *************************************************************************************************/

#ifndef __VERT_ARRAY_H__
#define __VERT_ARRAY_H__

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.hpp"
#define THRESHOLD_GRAPH_SIZE 20*1024*1024*1024

enum get_edge_state
{
    OUT_EDGE = 1, IN_EDGE
};
template <typename T>
class index_vert_array{
	private:
		std::string mmapped_vert_file;
		std::string mmapped_edge_file;
		int vert_index_file_fd;
		int edge_file_fd;
		unsigned long long vert_index_file_length;
		unsigned long long edge_file_length;
		vert_index* vert_array_header;
		T * edge_array_header;

        //if necessary
        std::string mmapped_in_vert_file;
        std::string mmapped_in_edge_file;
        int in_vert_index_file_fd;
        int in_edge_file_fd;
        unsigned long long in_vert_index_file_length;
        unsigned long long in_edge_file_length;
        vert_index * in_vert_array_header;
        in_edge * in_edge_array_header;


        u32_t segment_cap;
        const char* vert_attr_array; //vert_attr_array, used for get neighbour's attribute
        const char* vert_attr_buf;   //vert_attr_buf, used for get neighbour's attribute

	public:
		index_vert_array();
		~index_vert_array();
		//return the number of out edges of vid
		unsigned int num_out_edges( unsigned int vid);
        //mode: OUT_EDGE(1) or IN_EDGE(2)
        unsigned int num_edges(unsigned int vid, int mode);
		//return the "which"-th out edge of vid
		void get_out_edge( unsigned int vid, unsigned int which, T &ret);
        void get_in_edge(unsigned int vid, unsigned int which, in_edge &ret);
        u32_t get_out_neighbour( unsigned int vid, unsigned int which );
        u32_t get_in_neighbour( unsigned int vid, unsigned int which );

        void set_segment_cap(u32_t cap);
        void set_vert_attr_ptr(const char * va_array, const char * va_buf);

        template <typename VERT_ATTR> const VERT_ATTR * get_in_neigh_attr(u32_t vid, u32_t which)
        {
            /*
            if( which > index_vert_array<T>::num_edges( vid, IN_EDGE) )
            {
                //return NULL;
                PRINT_ERROR("vertex %d get_in_edge out of range.\n", vid);
            }
            */
            u32_t neigh_id = in_edge_array_header[ in_vert_array_header[vid].offset + which ].get_src_value();
            if(vid / segment_cap == neigh_id / segment_cap){
                return &(((VERT_ATTR*)vert_attr_buf)[neigh_id % segment_cap]);
            }
            else{
                return &(((VERT_ATTR*)vert_attr_array)[neigh_id]);
            }
        }
        template <typename VERT_ATTR> const VERT_ATTR * get_out_neigh_attr(u32_t vid, u32_t which)
        {
            /*
            if( which > index_vert_array<T>::num_edges( vid, OUT_EDGE) )
            {
                //return NULL;
                PRINT_ERROR("vertex %d get_out_edge out of range.\n", vid);
            }
            */
            u32_t neigh_id = edge_array_header[ vert_array_header[vid].offset + which ].get_dest_value();
            if(vid / segment_cap == neigh_id / segment_cap){
                return &(((VERT_ATTR*)vert_attr_buf)[neigh_id % segment_cap]);
            }
            else{
                return &(((VERT_ATTR*)vert_attr_array)[neigh_id]);
            }

        }
};
#endif
