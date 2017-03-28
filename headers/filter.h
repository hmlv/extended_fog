/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Declaration:
 *   The object for filter
 *************************************************************************************************/

#ifndef __FILTER_H__
#define __FILTER_H__

#include "index_vert_array.hpp"
#include "fog_program.h"

//template<typename VA>
template<typename VA, typename U, typename T>
class Filter
{
    public:
        void do_scc_filter(VA * va, int task_id);
        void do_trim_filter(VA * va, int task_id);
        void set_alg(Fog_program<VA, U, T>* alg_ptr);
        void do_filter(VA* va, int task_id);
    private:
        Fog_program<VA, U, T> * _alg_ptr;
};

#endif

