/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   base class of the algorithms
 *
 * Notes:
 *
 *************************************************************************************************/

#ifndef __FOG_PROGRAM_H__
#define __FOG_PROGRAM_H__

#include "index_vert_array.hpp"
typedef unsigned int u32_t;

enum algorithm_operation{
    SCATTER_GATHER = 0,
    UPDATE_VERTEX
};

//the typename T_EDGE is the edge file-type(type1 or type2)
template <typename VERT_ATTR, typename ALG_UPDATE, typename T_EDGE>
class Fog_program{
	public:
        /*
         * These variablies below must be set, beacuse the FOGP engine will use them
         * to control the algorithm.
         */
        u32_t num_tasks_to_sched;
        int forward_backward_phase;
        int CONTEXT_PHASE;
        int loop_counter;
        bool init_sched;
        bool set_forward_backward;
        bool need_all_neigh;
        int operation;

        Fog_program(int p_forward_backward_phase, bool p_init_sched, bool p_set_forward_backward)
        {
            forward_backward_phase = p_forward_backward_phase;
            init_sched = p_init_sched;
            set_forward_backward = p_set_forward_backward;
            CONTEXT_PHASE = 0;
            loop_counter = 0;
            num_tasks_to_sched = 0;
            need_all_neigh = false;
        }

        virtual ~Fog_program(){};

		//initialize each vertex of the graph
		virtual void init( u32_t vid, VERT_ATTR* this_vert, index_vert_array<T_EDGE> * vert_index ) = 0;

		//Notes:
		// 1) this member fuction will be used to scatter ONE edge of a vertex.
		// 2) the return value will be a pointer to the generated update.
		//	However, it is possible that no update will be generated at all!
		//	In that case, this member function should return NULL.
		// 3) This function should be "re-enterable", therefore, no global variables
		//	should be visited, or visited very carefully.
		virtual void scatter_one_edge(VERT_ATTR* this_vert,
					T_EDGE &this_edge, // type1 or type2 , only available for FORWARD_TRAVERSAL
					u32_t PARAMETER_BY_YOURSELF,
                    update<ALG_UPDATE> &u){};

		// Gather one update. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// va: the attribute of destination vertex;
		// u: the update from the "update" buffer.
		virtual void gather_one_update( u32_t vid, VERT_ATTR * vert_attr, update<ALG_UPDATE> * u ){};

        //A function before every iteration
        //This function will be used to print some important information about the algorithm
        virtual void before_iteration() = 0;

        //A function before every iteration
        virtual int after_iteration() = 0;

        //A function at the end.
        virtual int finalize(VERT_ATTR * va) = 0;

        virtual void update_vertex(u32_t vid, VERT_ATTR * this_vert, index_vert_array<T_EDGE>* vert_index){};

        virtual int num_of_remain_partitions(){return 0;};
        virtual int judge_for_filter(VERT_ATTR * this_vert){return 0;};

};


#endif
