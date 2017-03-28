/**************************************************************************************************
 * Authors:
 *   Huiming Lv
 *
 * Routines:
 *   The filter
 *
 * Notes:
 *
 *************************************************************************************************/
#ifndef __FILTER_CPP__
#define __FILTER_CPP__

#include "filter.h"
#include "convert.h"
#include "fog_adapter.h"

#define REMAP_BUFFER_LEN 2048*2048
//impletation of filter

//template<typename VA>
template<typename VA, typename U, typename T>
void Filter<VA, U, T>::do_scc_filter(VA * va, int task_id)
{
    struct bag_config FW_bag;
    struct bag_config BW_bag;
    struct bag_config RM_bag;
    std::stringstream result_id_stream;
    result_id_stream<<task_id;
    std::stringstream FW_id_stream;
    FW_id_stream<<(TASK_ID+1);
    std::stringstream BW_id_stream;
    BW_id_stream<<(TASK_ID+2);
    std::stringstream RM_id_stream;
    RM_id_stream<<(TASK_ID+3);
    //std::string result_file_name = gen_config.graph_path + result_id_stream.str(); + ".result";
    std::string FW_remap_file_name = gen_config.graph_path + "/temp" + FW_id_stream.str() + ".remap";
    std::string BW_remap_file_name = gen_config.graph_path + "/temp" + BW_id_stream.str() + ".remap";
    std::string RM_remap_file_name = gen_config.graph_path + "/temp" + RM_id_stream.str() + ".remap";

    int FW_remap_fd = open( FW_remap_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(-1 == FW_remap_fd)
    {
        printf("Cannot create remap file:%s\n Aborted.\n", FW_remap_file_name.c_str());
        exit(-1);
    }
    int BW_remap_fd = open( BW_remap_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(-1 == BW_remap_fd)
    {
        printf("Cannot create remap file:%s\n Aborted.\n", BW_remap_file_name.c_str());
        exit(-1);
    }
    int RM_remap_fd = open( RM_remap_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(-1 == RM_remap_fd)
    {
        printf("Cannot create remap file:%s\n Aborted.\n", RM_remap_file_name.c_str());
        exit(-1);
    }

    u32_t * FW_remap_buffer = new u32_t[REMAP_BUFFER_LEN];
    u32_t * BW_remap_buffer = new u32_t[REMAP_BUFFER_LEN];
    u32_t * RM_remap_buffer = new u32_t[REMAP_BUFFER_LEN];
    memset( (char*)FW_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
    memset( (char*)BW_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
    memset( (char*)RM_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );

    int FW_remap_buf_suffix = 0;
    int FW_remap_buf_offset = 0;
    int BW_remap_buf_suffix = 0;
    int BW_remap_buf_offset = 0;
    int RM_remap_buf_suffix = 0;
    int RM_remap_buf_offset = 0;

    int FW_vert_num = 0;
    int BW_vert_num = 0;
    int RM_vert_num = 0;
    int vert_to_scc = 0;
    int vert_to_TRIM = 0;

    for (unsigned int id = gen_config.min_vert_id; id <= gen_config.max_vert_id; id++ )
    {
        //PRINT_DEBUG_LOG("vertex %d, fw_bw_label=%d, is_found=%d\n", id, (va+id)->fw_bw_label,(va+id)->is_found);
        switch((va+id)->fw_bw_label)
        {
            case 1:
                {
                    //PRINT_DEBUG_TEST_LOG("%d\n",id);
                    FW_remap_buf_suffix = FW_vert_num - FW_remap_buf_offset*REMAP_BUFFER_LEN;
                    FW_remap_buffer[FW_remap_buf_suffix] = id;
                    if(FW_remap_buf_suffix == REMAP_BUFFER_LEN-1)
                    {
                        flush_buffer_to_file(FW_remap_fd, (char*)FW_remap_buffer, REMAP_BUFFER_LEN*sizeof(u32_t));
                        memset( (char*)FW_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
                        FW_remap_buf_offset++;
                    }
                    FW_vert_num++;
                    break;
                }

            case 2:
                {
                    if((va+id)->is_found)
                    {
                        vert_to_scc++;
                        //result should write back to disk
                        //PRINT_DEBUG_TEST_LOG("vertex %d is in a SCC!\n", id);
                    }
                    else
                    {
                        BW_remap_buf_suffix = BW_vert_num - BW_remap_buf_offset*REMAP_BUFFER_LEN;
                        BW_remap_buffer[BW_remap_buf_suffix] = id;
                        if(BW_remap_buf_suffix == REMAP_BUFFER_LEN-1)
                        {
                            flush_buffer_to_file(BW_remap_fd, (char*)BW_remap_buffer, REMAP_BUFFER_LEN*sizeof(u32_t));
                            memset( (char*)BW_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
                            BW_remap_buf_offset++;
                        }
                        BW_vert_num++;
                    }
                    break;
                }

            case 3:
                {
                    vert_to_TRIM++;
                    assert((va+id)->is_found==true);
                    //result should write back to disk
                    //PRINT_DEBUG_CV_LOG("TRIM.   %d is a SCC!\n", id);
                    break;
                }

            //case UINT_MAX:
            case 8:
                {
                    RM_remap_buf_suffix = RM_vert_num - RM_remap_buf_offset*REMAP_BUFFER_LEN;
                    RM_remap_buffer[RM_remap_buf_suffix] = id;
                    if(RM_remap_buf_suffix == REMAP_BUFFER_LEN-1)
                    {
                        flush_buffer_to_file(RM_remap_fd, (char*)RM_remap_buffer, REMAP_BUFFER_LEN*sizeof(u32_t));
                        memset( (char*)RM_remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
                        RM_remap_buf_offset++;
                    }
                    RM_vert_num++;
                    break;
                }

            default:
                PRINT_ERROR("vertex = %d, label = %d, WRONG!!!\n", id, (va+id)->fw_bw_label);
        }
    }

    flush_buffer_to_file(FW_remap_fd, (char*)FW_remap_buffer, (FW_vert_num - FW_remap_buf_offset*REMAP_BUFFER_LEN)*sizeof(u32_t));
    flush_buffer_to_file(BW_remap_fd, (char*)BW_remap_buffer, (BW_vert_num - BW_remap_buf_offset*REMAP_BUFFER_LEN)*sizeof(u32_t));
    flush_buffer_to_file(RM_remap_fd, (char*)RM_remap_buffer, (RM_vert_num - RM_remap_buf_offset*REMAP_BUFFER_LEN)*sizeof(u32_t));

    /*
    std::cout<<"FW: vert_num: "<<FW_vert_num<<std::endl;
    std::cout<<"BW: vert_num: "<<BW_vert_num<<std::endl;
    std::cout<<"RM: vert_num: "<<RM_vert_num<<std::endl;
    std::cout<<"vert_to_scc: "<<vert_to_scc<<std::endl;
    std::cout<<"vert_to_TRIM: "<<vert_to_TRIM<<std::endl;
    std::cout<<"all: "<<FW_vert_num+BW_vert_num+RM_vert_num+vert_to_scc+vert_to_TRIM<<std::endl;
    */
    PRINT_DEBUG_LOG("FW: vert_num: %d\n", FW_vert_num);
    PRINT_DEBUG_LOG("BW: vert_num: %d\n", BW_vert_num);
    PRINT_DEBUG_LOG("RM: vert_num: %d\n", RM_vert_num);
    PRINT_DEBUG_LOG("vert_to_scc: %d\n", vert_to_scc);
    PRINT_DEBUG_LOG("vert_to_TRIM: %d\n", vert_to_TRIM);

    delete [] FW_remap_buffer;
    delete [] BW_remap_buffer;
    delete [] RM_remap_buffer;

    FW_bag.creater_id = task_id;
    FW_bag.data_name = FW_remap_file_name;
    TASK_ID++;
    queue_task_id.push(TASK_ID);
    FW_bag.bag_id = TASK_ID;
    FW_bag.data_size = FW_vert_num;

    BW_bag.creater_id = task_id;
    BW_bag.data_name = BW_remap_file_name;
    TASK_ID++;
    queue_task_id.push(TASK_ID);
    BW_bag.bag_id = TASK_ID;
    BW_bag.data_size = BW_vert_num;

    RM_bag.creater_id = task_id;
    RM_bag.data_name = RM_remap_file_name;
    TASK_ID++;
    queue_task_id.push(TASK_ID);
    RM_bag.bag_id = TASK_ID;
    RM_bag.data_size = RM_vert_num;

    //queue_bag_config.push(FW_bag);
    //queue_bag_config.push(BW_bag);
    //queue_bag_config.push(RM_bag);
    //std::cout<<"queue size: "<<queue_bag_config.size()<<std::endl;
    task_bag_config_vec.push_back(FW_bag);
    task_bag_config_vec.push_back(BW_bag);
    task_bag_config_vec.push_back(RM_bag);
    std::cout<<"queue size: "<<task_bag_config_vec.size()<<std::endl;
    std::cout<<"scc filter over!"<<std::endl;
}

//template<typename VA>
template<typename VA, typename U, typename T>
void Filter<VA, U, T>::do_trim_filter(VA * va, int task_id)
{
    struct bag_config output_bag;
    std::stringstream result_id_stream;
    result_id_stream<<task_id;
    std::stringstream output_id_stream;
    output_id_stream<<(TASK_ID+1);
    //std::string result_file_name = gen_config.graph_path + result_id_stream.str(); + ".result";
    std::string remap_file_name = gen_config.graph_path + "/temp" + output_id_stream.str() + ".remap";

    int remap_fd = open( remap_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(-1 == remap_fd)
    {
        printf("Cannot create remap file:%s\n Aborted.\n", remap_file_name.c_str());
        exit(-1);
    }

    u32_t * remap_buffer = new u32_t[REMAP_BUFFER_LEN];
    memset( (char*)remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );

    int remap_buf_suffix = 0;
    int remap_buf_offset = 0;

    int vert_num = 0;
    int vert_to_TRIM = 0;

    for (unsigned int id = gen_config.min_vert_id; id <= gen_config.max_vert_id; id++ )
    {
        //PRINT_DEBUG_LOG("vertex %d, fw_bw_label=%d, is_found=%d\n", id, (va+id)->fw_bw_label,(va+id)->is_found);
        //if((va+id)->is_trim == false)
        if((va+id)->is_found == false)
        {
            //PRINT_DEBUG_TEST_LOG("%d\n",id);
            remap_buf_suffix = vert_num - remap_buf_offset*REMAP_BUFFER_LEN;
            remap_buffer[remap_buf_suffix] = id;
            if(remap_buf_suffix == REMAP_BUFFER_LEN-1)
            {
                flush_buffer_to_file(remap_fd, (char*)remap_buffer, REMAP_BUFFER_LEN*sizeof(u32_t));
                memset( (char*)remap_buffer, 0, REMAP_BUFFER_LEN*sizeof(u32_t) );
                remap_buf_offset++;
            }
            vert_num++;
        }
        else
        {
            //assert((va+id)->in_degree==0 || (va+id)->out_degree==0 );
            assert((va+id)->prev_root == (va+id)->component_root);
            vert_to_TRIM++;
        }
    }

    flush_buffer_to_file(remap_fd, (char*)remap_buffer, (vert_num - remap_buf_offset*REMAP_BUFFER_LEN)*sizeof(u32_t));

    PRINT_DEBUG("vert_to_TRIM: %d\n", vert_to_TRIM);
    PRINT_DEBUG("after trim, remian %d vertex\n", vert_num);
    PRINT_DEBUG("all = %d\n", vert_to_TRIM + vert_num);

    delete [] remap_buffer;

    output_bag.creater_id = task_id;
    output_bag.data_name = remap_file_name;
    TASK_ID++;
    queue_task_id.push(TASK_ID);
    output_bag.bag_id = TASK_ID;
    output_bag.data_size = vert_num;

    //queue_bag_config.push(output_bag);
    //std::cout<<"queue size: "<<queue_bag_config.size()<<std::endl;
    task_bag_config_vec.push_back(output_bag);
    std::cout<<"queue size: "<<task_bag_config_vec.size()<<std::endl;
    std::cout<<"trim filter over!"<<std::endl;
}

template<typename VA, typename U, typename T>
void Filter<VA, U, T>::set_alg(Fog_program<VA,U,T> * m_alg_ptr){
    this->_alg_ptr = m_alg_ptr;
}

template<typename VA, typename U, typename T>
void Filter<VA, U, T>::do_filter(VA *va, int task_id){
    int partition_num = this->_alg_ptr->num_of_remain_partitions();
    struct bag_config * bag_array = new struct bag_config[partition_num];
    int * fd_array = new int[partition_num];
    u32_t ** remap_buffer = new u32_t * [partition_num];
    int * remap_buf_suffix = new int[partition_num];
    int * remap_buf_offset = new int[partition_num];
    int * part_vert_num    = new int[partition_num];
    for(int i = 0; i < partition_num; ++i){
        std::stringstream part_id_stream;
        part_id_stream<<(TASK_ID + i+1);
        std::string part_remap_file_name = gen_config.graph_path + "/temp" + part_id_stream.str() +  ".remap";
        fd_array[i] = open(part_remap_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
        if(-1 == fd_array[i])
        {
            printf("Cannot create remap file:%s\n Aborted.\n", part_remap_file_name.c_str());
            exit(-1);
        }
        remap_buffer[i] = new u32_t[REMAP_BUFFER_LEN];
        memset((char*)remap_buffer[i], 0, REMAP_BUFFER_LEN*sizeof(u32_t));
        remap_buf_suffix[i] = 0;
        remap_buf_offset[i] = 0;
        part_vert_num[i]    = 0;
        bag_array[i].creater_id = task_id;
        bag_array[i].data_name  = part_remap_file_name;
    }

    int judge = 0;
    for (unsigned int id = gen_config.min_vert_id; id <= gen_config.max_vert_id; id++ )
    {
        judge = this->_alg_ptr->judge_for_filter(va+id);
        if(-1==judge){
            continue;
        }
        remap_buf_suffix[judge] = part_vert_num[judge] - remap_buf_offset[judge]*REMAP_BUFFER_LEN;
        remap_buffer[judge][remap_buf_suffix[judge]] = id;
        if(remap_buf_suffix[judge] == REMAP_BUFFER_LEN-1){
            flush_buffer_to_file(fd_array[judge], (char*)(remap_buffer[judge]), REMAP_BUFFER_LEN*sizeof(u32_t));
            memset((char*)remap_buffer[judge], 0, REMAP_BUFFER_LEN*sizeof(u32_t));
            remap_buf_offset[judge]++;
        }
        part_vert_num[judge]++;
    }

    for(int i = 0; i < partition_num; ++i){
        flush_buffer_to_file(fd_array[i], (char*)(remap_buffer[i]), (part_vert_num[i] - remap_buf_offset[i]*REMAP_BUFFER_LEN)*sizeof(u32_t));
        PRINT_DEBUG_LOG("the %dth partition: vert_num = %d\n", i, part_vert_num[i]);
        delete [] remap_buffer[i];
        TASK_ID++;
        queue_task_id.push(TASK_ID);
        bag_array[i].bag_id = TASK_ID;
        bag_array[i].data_size = part_vert_num[i];
        task_bag_config_vec.push_back(bag_array[i]);
    }
    delete []bag_array;
    delete []fd_array;
    delete []remap_buffer;
    delete []remap_buf_suffix;
    delete []remap_buf_offset;
    delete []part_vert_num;
    std::cout<<"queue size: "<<task_bag_config_vec.size()<<std::endl;
    std::cout<<"filter over!"<<std::endl;
}

/*
template<typename VA, typename T>
void Filter<VA, T>::do_scc_filter(VA * va, index_vert_array<T> * vertex_index)
{
    struct convert::edge * FW_edge_buffer = NULL;
    struct convert::edge * BW_edge_buffer = NULL;
    struct convert::edge * RM_edge_buffer = NULL;
    struct convert::type2_edge * type2_FW_edge_buffer = NULL;
    struct convert::type2_edge * type2_BW_edge_buffer = NULL;
    struct convert::type2_edge * type2_RM_edge_buffer = NULL;

    struct convert::vert_index * FW_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];
    struct convert::in_edge * FW_in_edge_buffer = new struct convert::in_edge[EDGE_BUFFER_LEN];
    struct convert::vert_index * FW_in_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];

    struct convert::vert_index * BW_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];
    struct convert::in_edge * BW_in_edge_buffer = new struct convert::in_edge[EDGE_BUFFER_LEN];
    struct convert::vert_index * BW_in_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];

    struct convert::vert_index * RM_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];
    struct convert::in_edge * RM_in_edge_buffer = new struct convert::in_edge[EDGE_BUFFER_LEN];
    struct convert::vert_index * RM_in_vert_buffer = new struct convert::vert_index[VERT_BUFFER_LEN];

    bool with_type1 = false;
    if(sizeof(T)==sizeof(type1_edge))
    {
        with_type1 = true;
    }

    if(with_type1)
    {
        FW_edge_buffer = new struct convert::edge[EDGE_BUFFER_LEN];
        BW_edge_buffer = new struct convert::edge[EDGE_BUFFER_LEN];
        RM_edge_buffer = new struct convert::edge[EDGE_BUFFER_LEN];
        memset((char*)FW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
        memset((char*)BW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
        memset((char*)RM_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
    }
    else
    {
        type2_FW_edge_buffer = new struct convert::type2_edge[EDGE_BUFFER_LEN];
        type2_BW_edge_buffer = new struct convert::type2_edge[EDGE_BUFFER_LEN];
        type2_RM_edge_buffer = new struct convert::type2_edge[EDGE_BUFFER_LEN];
        memset((char*)type2_FW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
        memset((char*)type2_BW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
        memset((char*)type2_RM_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
    }



    memset((char*)FW_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)FW_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)FW_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
    memset((char*)BW_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)BW_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)BW_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
    memset((char*)RM_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)RM_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    memset((char*)RM_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
    //std::cout<<"memset ok\n";
    // debug
    u64_t vert_to_scc = 0;
    u64_t vert_to_TRIM = 0;
    // debug end

    int FW_edge_fd = 0;
    int FW_index_fd = 0;
    int FW_in_edge_fd = 0;
    int FW_in_index_fd = 0;

    int BW_edge_fd = 0;
    int BW_index_fd = 0;
    int BW_in_edge_fd = 0;
    int BW_in_index_fd = 0;

    int RM_edge_fd = 0;
    int RM_index_fd = 0;
    int RM_in_edge_fd = 0;
    int RM_in_index_fd = 0;
    //std::cout<<"test()!!!!!!!!!!!\n";

    //std::cout<<va<<std::endl;
    std::string temp_file_name = vm["graph"].as<std::string>();
    temp_file_name = temp_file_name.substr(temp_file_name.find_last_of("/")+1);
    temp_file_name = temp_file_name.substr(0, temp_file_name.find_last_of("."));
    //std::cout<<temp_file_name<<std::endl;
    std::string FW_edge_file = temp_file_name + ".FW_edge";
    std::string FW_index_file = temp_file_name + ".FW_index";
    std::string FW_in_edge_file = temp_file_name + ".FW_in_edge";
    std::string FW_in_index_file = temp_file_name + ".FW_in_index";
    std::string BW_edge_file = temp_file_name + ".BW_edge";
    std::string BW_index_file = temp_file_name + ".BW_index";
    std::string BW_in_edge_file = temp_file_name + ".BW_in_edge";
    std::string BW_in_index_file = temp_file_name + ".BW_in_index";
    std::string RM_edge_file = temp_file_name + ".RM_edge";
    std::string RM_index_file = temp_file_name + ".RM_index";
    std::string RM_in_edge_file = temp_file_name + ".RM_in_edge";
    std::string RM_in_index_file = temp_file_name + ".RM_in_index";

    FW_edge_fd = open(FW_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(FW_edge_fd == -1)
    {
        printf("Cannot create FW_edge_file:%s\nAborted..\n", FW_edge_file.c_str());
        exit(-1);
    }
    FW_index_fd = open(FW_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(FW_index_fd == -1)
    {
        printf("Cannot create FW_index_file:%s\nAborted..\n", FW_index_file.c_str());
        exit(-1);
    }
    FW_in_edge_fd = open(FW_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(FW_in_edge_fd == -1)
    {
        printf("Cannot create FW_in_edge_file:%s\nAborted..\n", FW_in_edge_file.c_str());
        exit(-1);
    }
    FW_in_index_fd = open(FW_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(FW_in_index_fd == -1)
    {
        printf("Cannot create FW_in_index_file:%s\nAborted..\n", FW_in_index_file.c_str());
        exit(-1);
    }
    BW_edge_fd = open(BW_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(BW_edge_fd == -1)
    {
        printf("Cannot create BW_edge_file:%s\nAborted..\n", BW_edge_file.c_str());
        exit(-1);
    }
    BW_index_fd = open(BW_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(BW_index_fd == -1)
    {
        printf("Cannot create BW_index_file:%s\nAborted..\n", BW_index_file.c_str());
        exit(-1);
    }
    BW_in_edge_fd = open(BW_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(BW_in_edge_fd == -1)
    {
        printf("Cannot create BW_in_edge_file:%s\nAborted..\n", BW_in_edge_file.c_str());
        exit(-1);
    }
    BW_in_index_fd = open(BW_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(BW_in_index_fd == -1)
    {
        printf("Cannot create BW_in_index_file:%s\nAborted..\n", BW_in_index_file.c_str());
        exit(-1);
    }
    RM_edge_fd = open(RM_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(RM_edge_fd == -1)
    {
        printf("Cannot create RM_edge_file:%s\nAborted..\n", RM_edge_file.c_str());
        exit(-1);
    }
    RM_index_fd = open(RM_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(RM_index_fd == -1)
    {
        printf("Cannot create RM_index_file:%s\nAborted..\n", RM_index_file.c_str());
        exit(-1);
    }
    RM_in_edge_fd = open(RM_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(RM_in_edge_fd == -1)
    {
        printf("Cannot create RM_in_edge_file:%s\nAborted..\n", RM_in_edge_file.c_str());
        exit(-1);
    }
    RM_in_index_fd = open(RM_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(RM_in_index_fd == -1)
    {
        printf("Cannot create RM_in_index_file:%s\nAborted..\n", RM_in_index_file.c_str());
        exit(-1);
    }

    //std::cout<<"open file OK!\n";

    std::vector<VA> FW_vec;
    std::vector<VA> BW_vec;
    std::vector<VA> RM_vec;

    u64_t FW_vert_num = 0;
    u32_t FW_vert_suffix = 0;
    u32_t FW_vert_buffer_offset = 0;
    u64_t FW_edge_num = 0;
    u32_t FW_edge_suffix = 0;
    u32_t FW_edge_buffer_offset = 0;
    u64_t recent_FW_edge_num = 0;
    u64_t FW_in_edge_num = 0;
    u32_t FW_in_edge_suffix = 0;
    u32_t FW_in_edge_buffer_offset = 0;
    u64_t recent_FW_in_edge_num = 0;

    u64_t BW_vert_num = 0;
    u32_t BW_vert_suffix = 0;
    u32_t BW_vert_buffer_offset = 0;
    u64_t BW_edge_num = 0;
    u32_t BW_edge_suffix = 0;
    u32_t BW_edge_buffer_offset = 0;
    u64_t recent_BW_edge_num = 0;
    u64_t BW_in_edge_num = 0;
    u32_t BW_in_edge_suffix = 0;
    u32_t BW_in_edge_buffer_offset = 0;
    u64_t recent_BW_in_edge_num = 0;

    u64_t RM_vert_num = 0;
    u32_t RM_vert_suffix = 0;
    u32_t RM_vert_buffer_offset = 0;
    u64_t RM_edge_num = 0;
    u32_t RM_edge_suffix = 0;
    u32_t RM_edge_buffer_offset = 0;
    u64_t recent_RM_edge_num = 0;
    u64_t RM_in_edge_num = 0;
    u32_t RM_in_edge_suffix = 0;
    u32_t RM_in_edge_buffer_offset = 0;
    u64_t recent_RM_in_edge_num = 0;

    T * temp_out_edge = new T;
    fog::in_edge * temp_in_edge = new in_edge;

    u32_t temp_for_degree = 0;
    u32_t temp_for_dst_or_src = 0;


    for (unsigned int id = gen_config.min_vert_id; id <= gen_config.max_vert_id; id++ )
    {
        //std::cout<<"for id = "<<id<<std::endl;
        PRINT_DEBUG_LOG("vertex %d, fw_bw_label=%d, is_found=%d\n", id, (va+id)->fw_bw_label,(va+id)->is_found);
        switch((va+id)->fw_bw_label)
        {
            case 1:
                {
                    FW_vert_num++;
                    FW_vec.push_back(*(va+id));
                    temp_for_degree = vertex_index->num_edges(id, OUT_EDGE);
                    for(u32_t i = 0; i < temp_for_degree; i++)
                    {
                        temp_out_edge = vertex_index->get_out_edge(id, i);
                        temp_for_dst_or_src = temp_out_edge->get_dest_value();
                        if(1 == (va+temp_for_dst_or_src)->fw_bw_label)
                        {
                            FW_edge_num++;
                            FW_edge_suffix = FW_edge_num - (FW_edge_buffer_offset*EDGE_BUFFER_LEN);
                            if(with_type1)
                            {
                                FW_edge_buffer[FW_edge_suffix].dest_vert = temp_for_dst_or_src;
                                FW_edge_buffer[FW_edge_suffix].edge_weight = temp_out_edge->get_edge_value();
                            }
                            else
                            {
                                type2_FW_edge_buffer[FW_edge_suffix].dest_vert = temp_for_dst_or_src;
                            }

                            if(FW_edge_suffix == EDGE_BUFFER_LEN-1)
                            {
                                //std::cout<<"write\n";
                                if(with_type1)
                                {
                                    flush_buffer_to_file(FW_edge_fd, (char*)FW_edge_buffer,
                                            EDGE_BUFFER_LEN*sizeof(convert::edge));
                                    memset((char*)FW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
                                }
                                else
                                {
                                    flush_buffer_to_file(FW_edge_fd, (char*)type2_FW_edge_buffer,
                                            EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                    memset((char*)type2_FW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                }

                                FW_edge_buffer_offset++;
                            }
                        }
                    }

                    FW_vert_suffix = FW_vert_num - 1 - FW_vert_buffer_offset*VERT_BUFFER_LEN;
                    if(FW_edge_num != recent_FW_edge_num)
                    {
                        FW_vert_buffer[FW_vert_suffix].offset = recent_FW_edge_num+1;
                        recent_FW_edge_num = FW_edge_num;
                    }

                    temp_for_degree = vertex_index->num_edges(id, IN_EDGE);
                    for(u32_t i = 0; i < temp_for_degree; i++)
                    {
                        temp_in_edge = vertex_index->get_in_edge(id, i);
                        temp_for_dst_or_src = temp_in_edge->get_src_value();
                        if(1 == (va+temp_for_dst_or_src)->fw_bw_label)
                        {
                            FW_in_edge_num++;
                            FW_in_edge_suffix = FW_in_edge_num - FW_in_edge_buffer_offset*EDGE_BUFFER_LEN;
                            FW_in_edge_buffer[FW_in_edge_suffix].in_vert = temp_for_dst_or_src;
                            if(FW_in_edge_suffix == EDGE_BUFFER_LEN-1)
                            {
                                flush_buffer_to_file(FW_in_edge_fd, (char*)FW_in_edge_buffer,
                                        EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                memset((char*)FW_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                FW_in_edge_buffer_offset++;
                            }
                        }
                    }
                    if(FW_in_edge_num != recent_FW_in_edge_num)
                    {
                        FW_in_vert_buffer[FW_vert_suffix].offset = recent_FW_in_edge_num+1;
                        recent_FW_in_edge_num = FW_in_edge_num;
                    }
                    if(FW_vert_suffix == VERT_BUFFER_LEN-1)
                    {
                        //std::cout<<"write\n";
                        flush_buffer_to_file(FW_index_fd, (char*)FW_vert_buffer,
                                VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        memset((char*)FW_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                        flush_buffer_to_file(FW_in_index_fd, (char*)FW_in_vert_buffer,
                                VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        memset((char*)FW_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                        FW_vert_buffer_offset++;
                    }
                    break;
                }
            case 2:
                {
                    if((va+id)->is_found)
                    {
                        vert_to_scc++;
                        PRINT_DEBUG_TEST_LOG("vertex %d is in a SCC!\n", id);
                    }
                    else
                    {
                        BW_vert_num++;
                        BW_vec.push_back(*(va+id));
                        temp_for_degree = vertex_index->num_edges(id, OUT_EDGE);
                        for(u32_t i = 0; i < temp_for_degree; i++)
                        {
                            temp_out_edge = vertex_index->get_out_edge(id, i);
                            temp_for_dst_or_src = temp_out_edge->get_dest_value();
                            if(2 == (va+temp_for_dst_or_src)->fw_bw_label)
                            {
                                BW_edge_num++;
                                BW_edge_suffix = BW_edge_num - BW_edge_buffer_offset*EDGE_BUFFER_LEN;
                                if(with_type1)
                                {
                                    BW_edge_buffer[BW_edge_suffix].dest_vert = temp_for_dst_or_src;
                                    BW_edge_buffer[BW_edge_suffix].edge_weight = temp_out_edge->get_edge_value();
                                }
                                else
                                {
                                    type2_BW_edge_buffer[BW_edge_suffix].dest_vert = temp_for_dst_or_src;
                                }

                                if(BW_edge_suffix == EDGE_BUFFER_LEN-1)
                                {
                                    //std::cout<<"write\n";
                                    if(with_type1)
                                    {
                                        flush_buffer_to_file(BW_edge_fd, (char*)BW_edge_buffer,
                                                EDGE_BUFFER_LEN*sizeof(convert::edge));
                                        memset((char*)BW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
                                    }
                                    else
                                    {
                                        flush_buffer_to_file(BW_edge_fd, (char*)type2_BW_edge_buffer,
                                                EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                        memset((char*)type2_BW_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                    }
                                    BW_edge_buffer_offset++;
                                }
                            }
                        }

                        BW_vert_suffix = BW_vert_num - 1 - BW_vert_buffer_offset*VERT_BUFFER_LEN;
                        if(BW_edge_num != recent_BW_edge_num)
                        {
                            BW_vert_buffer[BW_vert_suffix].offset = recent_BW_edge_num + 1;
                            recent_BW_edge_num = BW_edge_num;
                        }

                        temp_for_degree = vertex_index->num_edges(id, IN_EDGE);
                        for(u32_t i = 0; i < temp_for_degree; i++)
                        {
                            temp_in_edge = vertex_index->get_in_edge(id, i);
                            temp_for_dst_or_src = temp_in_edge->get_src_value();
                            if(2 == (va+temp_for_dst_or_src)->fw_bw_label)
                            {
                                BW_in_edge_num++;
                                BW_in_edge_suffix = BW_in_edge_num - BW_in_edge_buffer_offset*EDGE_BUFFER_LEN;
                                BW_in_edge_buffer[BW_in_edge_suffix].in_vert = temp_for_dst_or_src;
                                if(BW_in_edge_suffix == EDGE_BUFFER_LEN-1)
                                {
                                    //std::cout<<"write\n";
                                    flush_buffer_to_file(BW_in_edge_fd, (char*)BW_in_edge_buffer,
                                            EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                    memset((char*)BW_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                    BW_in_edge_buffer_offset++;
                                }
                            }
                        }

                        if(BW_in_edge_num != recent_BW_in_edge_num)
                        {
                            BW_in_vert_buffer[BW_vert_suffix].offset = recent_BW_in_edge_num + 1;
                            recent_BW_in_edge_num = BW_in_edge_num;
                        }

                        if(BW_vert_suffix == VERT_BUFFER_LEN-1)
                        {
                            //std::cout<<"write\n";
                            flush_buffer_to_file(BW_index_fd, (char*)BW_vert_buffer,
                                    VERT_BUFFER_LEN*sizeof(convert::vert_index));
                            memset((char*)BW_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                            flush_buffer_to_file(BW_in_index_fd, (char*)BW_in_vert_buffer,
                                    VERT_BUFFER_LEN*sizeof(convert::vert_index));
                            memset((char*)BW_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                            BW_vert_buffer_offset++;
                        }
                    }
                    break;
                }
            case 3:
                {
                    vert_to_TRIM++;
                    assert((va+id)->is_found==true);
                    PRINT_DEBUG_CV_LOG("TRIM.   %d is a SCC!\n", id);
                    break;
                }
            case UINT_MAX:
                {
                    RM_vert_num++;
                    RM_vec.push_back(*(va+id));
                    temp_for_degree = vertex_index->num_edges(id, OUT_EDGE);
                    for(u32_t i = 0; i < temp_for_degree; i++)
                    {
                        temp_out_edge = vertex_index->get_out_edge(id, i);
                        temp_for_dst_or_src = temp_out_edge->get_dest_value();
                        if(UINT_MAX == (va+temp_for_dst_or_src)->fw_bw_label)
                        {
                            RM_edge_num++;
                            RM_edge_suffix = RM_edge_num - RM_edge_buffer_offset*EDGE_BUFFER_LEN;
                            if(with_type1)
                            {
                                RM_edge_buffer[RM_edge_suffix].dest_vert = temp_for_dst_or_src;
                                RM_edge_buffer[RM_edge_suffix].edge_weight = temp_out_edge->get_edge_value();
                            }
                            else
                            {
                                type2_RM_edge_buffer[RM_edge_suffix].dest_vert = temp_for_dst_or_src;
                            }

                            if(RM_edge_suffix == EDGE_BUFFER_LEN-1)
                            {
                                //std::cout<<"write\n";
                                if(with_type1)
                                {
                                    flush_buffer_to_file(RM_edge_fd, (char*)RM_edge_buffer,
                                            EDGE_BUFFER_LEN*sizeof(convert::edge));
                                    memset((char*)RM_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::edge));
                                }
                                else
                                {
                                    flush_buffer_to_file(RM_edge_fd, (char*)type2_RM_edge_buffer,
                                            EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                    memset((char*)type2_RM_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                                }
                                RM_edge_buffer_offset++;
                            }
                        }
                    }

                    RM_vert_suffix = RM_vert_num - 1 - RM_vert_buffer_offset*VERT_BUFFER_LEN;
                    if(RM_edge_num != recent_RM_edge_num)
                    {
                        RM_vert_buffer[RM_vert_suffix].offset = recent_RM_edge_num + 1;
                        recent_RM_edge_num = RM_edge_num;
                    }

                    temp_for_degree = vertex_index->num_edges(id, IN_EDGE);
                    for(u32_t i = 0; i < temp_for_degree; i++)
                    {
                        temp_in_edge = vertex_index->get_in_edge(id, i);
                        temp_for_dst_or_src = temp_in_edge->get_src_value();
                        if(UINT_MAX == (va+temp_for_dst_or_src)->fw_bw_label)
                        {
                            RM_in_edge_num++;
                            RM_in_edge_suffix = RM_in_edge_num - RM_in_edge_buffer_offset*EDGE_BUFFER_LEN;
                            RM_in_edge_buffer[RM_in_edge_suffix].in_vert = temp_for_dst_or_src;
                            if(RM_in_edge_suffix == EDGE_BUFFER_LEN-1)
                            {
                                //std::cout<<"write\n";
                                flush_buffer_to_file(RM_in_edge_fd, (char*)RM_in_edge_buffer,
                                        EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                memset((char*)RM_in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                                RM_in_edge_buffer_offset++;
                            }
                        }
                    }

                    if(RM_in_edge_num != recent_RM_in_edge_num)
                    {
                        RM_in_vert_buffer[RM_vert_suffix].offset = recent_RM_in_edge_num + 1;
                        recent_RM_in_edge_num = RM_in_edge_num;
                    }

                    if(RM_vert_suffix == VERT_BUFFER_LEN-1)
                    {
                        //std::cout<<"write\n";
                        flush_buffer_to_file(RM_index_fd, (char*)RM_vert_buffer,
                                VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        memset((char*)RM_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                        flush_buffer_to_file(RM_in_index_fd, (char*)RM_in_vert_buffer,
                                VERT_BUFFER_LEN*sizeof(convert::vert_index));
                        memset((char*)RM_in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(convert::vert_index));

                        RM_vert_buffer_offset++;
                    }
                    break;
                }

            default:
                PRINT_ERROR("WRONG!!!\n");
        }
    }

    //std::cout<<"last write\n";
    flush_buffer_to_file(FW_index_fd, (char*)FW_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    flush_buffer_to_file(FW_in_index_fd, (char*)FW_in_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    flush_buffer_to_file(BW_index_fd, (char*)BW_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    flush_buffer_to_file(BW_in_index_fd, (char*)BW_in_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    flush_buffer_to_file(RM_index_fd, (char*)RM_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));
    flush_buffer_to_file(RM_in_index_fd, (char*)RM_in_vert_buffer, VERT_BUFFER_LEN*sizeof(convert::vert_index));

    if(with_type1)
    {
        flush_buffer_to_file(FW_edge_fd, (char*)FW_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::edge));
        flush_buffer_to_file(BW_edge_fd, (char*)BW_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::edge));
        flush_buffer_to_file(RM_edge_fd, (char*)RM_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::edge));
    }
    else
    {
        flush_buffer_to_file(FW_edge_fd, (char*)type2_FW_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
        flush_buffer_to_file(BW_edge_fd, (char*)type2_BW_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
        flush_buffer_to_file(RM_edge_fd, (char*)type2_RM_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
    }

    flush_buffer_to_file(FW_in_edge_fd, (char*)FW_in_edge_buffer,
            EDGE_BUFFER_LEN*sizeof(convert::in_edge));
    flush_buffer_to_file(BW_in_edge_fd, (char*)BW_in_edge_buffer,
            EDGE_BUFFER_LEN*sizeof(convert::in_edge));
    flush_buffer_to_file(RM_in_edge_fd, (char*)RM_in_edge_buffer,
            EDGE_BUFFER_LEN*sizeof(convert::in_edge));

    std::cout<<"FW: vert_num: "<<FW_vert_num<<" edge_num:"<<FW_edge_num<<std::endl;
    std::cout<<"BW: vert_num: "<<BW_vert_num<<" edge_num:"<<BW_edge_num<<std::endl;
    std::cout<<"RM: vert_num: "<<RM_vert_num<<" edge_num:"<<RM_edge_num<<std::endl;
    std::cout<<"vert_to_scc: "<<vert_to_scc<<std::endl;
    std::cout<<"vert_to_TRIM: "<<vert_to_TRIM<<std::endl;
    std::cout<<"all: "<<FW_vert_num+BW_vert_num+RM_vert_num+vert_to_scc+vert_to_TRIM<<std::endl;

    if(with_type1)
    {
        delete [] FW_edge_buffer;
        delete [] BW_edge_buffer;
        delete [] RM_edge_buffer;
    }
    else
    {
        delete [] type2_FW_edge_buffer;
        delete [] type2_BW_edge_buffer;
        delete [] type2_RM_edge_buffer;
    }

    delete [] FW_vert_buffer;
    delete [] FW_in_edge_buffer;
    delete [] FW_in_vert_buffer;

    delete [] BW_vert_buffer;
    delete [] BW_in_edge_buffer;
    delete [] BW_in_vert_buffer;

    delete [] RM_vert_buffer;
    delete [] RM_in_edge_buffer;
    delete [] RM_in_vert_buffer;

    std::cout<<"filter over!"<<std::endl;


}
*/

#endif
