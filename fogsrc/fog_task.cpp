/**************************************************************************************************
 * Authors: 
 *   Huiming Lv 
 *
 * Declaration:
 *
 *************************************************************************************************/


#include "fog_task.h"
#include "convert.h"
#include <sstream>
#include <fstream>
#include <limits.h>
#include "fog_adapter.h"
#define REMAP_EDGE_BUFFER_LEN 2048*2048
#define REMAP_VERT_BUFFER_LEN 2048*2048

template<typename VA, typename U, typename T>
Fog_task<VA, U, T>::Fog_task()
{
}

template<typename VA, typename U, typename T>
std::string Fog_task<VA, U, T>::create_dataset_sequence(struct bag_config * vert_bag_config, index_vert_array<T> * vertex_index)
{
    PRINT_DEBUG_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //PRINT_DEBUG_TEST_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //PRINT_DEBUG_CV_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //lvhuiming debug
    //if(this->get_task_id() == 6)
    //{
    /*
    FILE * debug_out_edge_file;
    FILE * debug_in_edge_file;
    std::string out_edge_file_name = "DEBUG_out_edge_file.txt";
    std::string in_edge_file_name = "DEBUG_in_edge_file.txt";
    debug_out_edge_file = fopen(out_edge_file_name.c_str(), "wt+");
    debug_in_edge_file = fopen(in_edge_file_name.c_str(), "wt+");
    */
    //}
    //debug end

    bool with_type1 = false;
    if(sizeof(T)==sizeof(type1_edge))
    {
        with_type1 = true;
    }
    bool with_in_edge = gen_config.with_in_edge; 

    remap_array_header = (u32_t * )map_vert_remap_file(vert_bag_config->data_name);
    /*
       for(int i = 0 ;i < vert_bag_config->data_size; i++)
       {
       PRINT_DEBUG_CV_LOG("%d\n",remap_array_header[i]);
       }
       */

    struct convert::edge * REMAP_edge_buffer = NULL; 
    struct convert::type2_edge * type2_REMAP_edge_buffer = NULL;
    if(with_type1)
    {
        REMAP_edge_buffer = new struct convert::edge[REMAP_EDGE_BUFFER_LEN];
        memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
    }
    else
    {
        type2_REMAP_edge_buffer = new struct convert::type2_edge[REMAP_EDGE_BUFFER_LEN];
        memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
    }

    struct convert::vert_index * REMAP_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
    memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

    struct convert::in_edge * REMAP_in_edge_buffer = NULL;
    struct convert::vert_index * REMAP_in_vert_buffer = NULL;
    if(with_in_edge)
    {
        REMAP_in_edge_buffer = new struct convert::in_edge[REMAP_EDGE_BUFFER_LEN];
        REMAP_in_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
        memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
        memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
    }
    //std::cout<<"memset ok\n";

    int REMAP_edge_fd = 0;
    int REMAP_index_fd = 0;
    int REMAP_in_edge_fd = 0;
    int REMAP_in_index_fd = 0;
    std::ofstream REMAP_desc_ofstream;

    //std::string temp_file_name = vm["graph"].as<std::string>();
    //temp_file_name = temp_file_name.substr(temp_file_name.find_last_of("/")+1);
    //temp_file_name = temp_file_name.substr(0, temp_file_name.find_last_of("."));
    std::string temp_file_name = gen_config.graph_path + "/temp";
    //std::cout<<temp_file_name<<std::endl;
    std::stringstream tmp_str_stream;
    tmp_str_stream<<(vert_bag_config->bag_id);
    std::string REMAP_edge_file     = temp_file_name + tmp_str_stream.str() + ".edge"    ;
    std::string REMAP_index_file    = temp_file_name + tmp_str_stream.str() + ".index"   ;
    std::string REMAP_in_edge_file  = temp_file_name + tmp_str_stream.str() + ".in-edge" ;
    std::string REMAP_in_index_file = temp_file_name + tmp_str_stream.str() + ".in-index";
    std::string REMAP_desc_file     = temp_file_name + tmp_str_stream.str() + ".desc"    ; 

    REMAP_edge_fd = open(REMAP_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(REMAP_edge_fd == -1)
    {
        printf("Cannot create REMAP_edge_file:%s\nAborted..\n", REMAP_edge_file.c_str());
        exit(-1);
    }
    REMAP_index_fd = open(REMAP_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(REMAP_index_fd == -1)
    {
        printf("Cannot create REMAP_index_file:%s\nAborted..\n", REMAP_index_file.c_str());
        exit(-1);
    }
    if(with_in_edge)
    {
        REMAP_in_edge_fd = open(REMAP_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
        if(REMAP_in_edge_fd == -1)
        {
            printf("Cannot create REMAP_in_edge_file:%s\nAborted..\n", REMAP_in_edge_file.c_str());
            exit(-1);
        }
        REMAP_in_index_fd = open(REMAP_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
        if(REMAP_in_index_fd == -1)
        {
            printf("Cannot create REMAP_in_index_file:%s\nAborted..\n", REMAP_in_index_file.c_str());
            exit(-1);
        }
    }

    std::cout<<"open file OK!\n";
    
    //u64_t REMAP_vert_num = 0;
    u32_t REMAP_vert_suffix = 0;
    u32_t REMAP_vert_buffer_offset = 0;
    u64_t REMAP_edge_num = 0;
    u32_t REMAP_edge_suffix = 0;
    u32_t REMAP_edge_buffer_offset = 0;
    u64_t recent_REMAP_edge_num = 0;
    u64_t REMAP_in_edge_num = 0;
    u32_t REMAP_in_edge_suffix = 0;
    u32_t REMAP_in_edge_buffer_offset = 0;
    u64_t recent_REMAP_in_edge_num = 0;

    //T * temp_out_edge = new T;
    T temp_out_edge;
    fog::in_edge temp_in_edge;

    u32_t temp_for_degree = 0;
    u32_t temp_for_dst_or_src = 0;

    u32_t old_vert_id = 0;
    u32_t new_vert_id = 0;

    //int left = 0;
    //int right = vert_bag_config->data_size;
    //int * location = new int[vert_bag_config->data_size];
    int * location = new int[gen_config.max_vert_id + 1];
    for (int s = 0; s <= gen_config.max_vert_id; s++)
    {
        location[s] = UINT_MAX;
    }
    for (int j = 0; j < vert_bag_config->data_size; j++)
    {
        location[remap_array_header[j]] = j;
    }
    

    std::cout<<vert_bag_config->data_size;
    for (int i = 0; i < vert_bag_config->data_size; i++ )
    {
        //std::cout<<i<<std::endl;
        //REMAP_vert_num++;
        old_vert_id = remap_array_header[i];
        temp_for_degree = vertex_index->num_edges(old_vert_id, OUT_EDGE);
        for(u32_t j = 0; j < temp_for_degree; j++) 
        {
            //temp_out_edge = vertex_index->get_out_edge(old_vert_id, j);
            vertex_index->get_out_edge(old_vert_id, j, temp_out_edge);
            //lvhuiming debug
            /*
            if(this->get_task_id() == 6 && i==vert_bag_config->data_size-2)
            {
                std::cout<<"task 6, vert:"<<remap_array_header[vert_bag_config->data_size-2]<<" outdegree = "<<temp_for_degree<<std::endl;
                temp_for_dst_or_src = temp_out_edge->get_dest_value();
                new_vert_id = fog_sequence_search(remap_array_header, left, right, temp_for_dst_or_src);
                std::cout<<"old desc: "<<temp_for_dst_or_src<<std::endl;
                std::cout<<"new desc: "<<new_vert_id<<std::endl;
            }
            */
            //debug end
            temp_for_dst_or_src = temp_out_edge.get_dest_value();
            //new_vert_id = fog_sequence_search(remap_array_header, left, right, temp_for_dst_or_src);
            new_vert_id = location[temp_for_dst_or_src];
            if(UINT_MAX != new_vert_id)
            {
                //lvhuiming debug
                /*
                if(this->get_task_id() == 6)
                {
                    fprintf(debug_out_edge_file, "%d\t%d\n", old_vert_id, temp_for_dst_or_src);
                }
                */
                //debug end
                REMAP_edge_num++;
                REMAP_edge_suffix = REMAP_edge_num - (REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN);
                if(with_type1)
                {
                    REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id; 
                    REMAP_edge_buffer[REMAP_edge_suffix].edge_weight = temp_out_edge.get_edge_value();
                }
                else
                {
                    type2_REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id;
                }

                if(REMAP_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                {
                    if(with_type1)
                    {
                        flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                        memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                    }
                    else
                    {
                        flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                        memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                    }

                    REMAP_edge_buffer_offset++;
                }
            }
        }

        REMAP_vert_suffix = i - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN;
        
        if(REMAP_edge_num != recent_REMAP_edge_num)
        {
            REMAP_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_edge_num+1;
            recent_REMAP_edge_num = REMAP_edge_num;
        }
        /* debug
        if(this->task_id==6)
        {
            PRINT_DEBUG_TEST_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_vert_buffer[REMAP_vert_suffix].offset);
        }
        */

        if(with_in_edge)
        {
            temp_for_degree = vertex_index->num_edges(old_vert_id, IN_EDGE);
            for(u32_t j = 0; j < temp_for_degree; j++)
            {
                //temp_in_edge = vertex_index->get_in_edge(old_vert_id, j);
                vertex_index->get_in_edge(old_vert_id, j, temp_in_edge);
                //lvhuiming debug
                /*
                if(this->get_task_id() == 6 && i==42014)
                {            
                    std::cout<<"task 6, vert:"<<remap_array_header[42014]<<" indegree = "<<temp_for_degree<<std::endl;
                    temp_for_dst_or_src = temp_in_edge->get_src_value();
                    new_vert_id = fog_sequence_search(remap_array_header, left, right, temp_for_dst_or_src);
                    std::cout<<"old desc: "<<temp_for_dst_or_src<<std::endl;
                    std::cout<<"new desc: "<<new_vert_id<<std::endl;
                }
                */
                //debug end
                temp_for_dst_or_src = temp_in_edge.get_src_value();
                //new_vert_id = fog_sequence_search(remap_array_header, left, right, temp_for_dst_or_src);
                new_vert_id = location[temp_for_dst_or_src];
                if(UINT_MAX != new_vert_id)
                {
                    //lvhuiming debug
                    /*
                    if(this->get_task_id() == 6)
                    {
                        fprintf(debug_in_edge_file, "%d\t%d\n", temp_for_dst_or_src, old_vert_id);
                    }
                    */
                    //debug end
                    REMAP_in_edge_num++;
                    REMAP_in_edge_suffix = REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN;
                    REMAP_in_edge_buffer[REMAP_in_edge_suffix].in_vert = new_vert_id;
                    if(REMAP_in_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                    {
                        flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                        memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                        REMAP_in_edge_buffer_offset++;
                    }
                }
            }
            if(REMAP_in_edge_num != recent_REMAP_in_edge_num)
            {
                REMAP_in_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_in_edge_num+1;
                recent_REMAP_in_edge_num = REMAP_in_edge_num;
            }
            /* debug
            if(this->task_id==6)
            {
                PRINT_DEBUG_CV_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_in_vert_buffer[REMAP_vert_suffix].offset);
            }
            */
        }

        if(REMAP_vert_suffix == REMAP_VERT_BUFFER_LEN-1)
        {
            flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
                    REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
            memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

            if(with_in_edge)
            {
                flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                        REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
            }

            REMAP_vert_buffer_offset++;
        }
    }
    delete [] location;

    std::cout<<"for over!\n";


    if(with_type1)
    {
        //flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));
        flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));

        delete [] REMAP_edge_buffer;
        
    }
    else
    {
        //flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));
        flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));
        
        delete [] type2_REMAP_edge_buffer;
    }

    flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
            (vert_bag_config->data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));
    delete [] REMAP_vert_buffer;
    
    close(REMAP_edge_fd);
    close(REMAP_index_fd);

    if(with_in_edge)
    {
        //flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
        flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                (1 + REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
        flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                (vert_bag_config->data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));

        delete [] REMAP_in_edge_buffer;
        delete [] REMAP_in_vert_buffer;

        close(REMAP_in_edge_fd);
        close(REMAP_in_index_fd);
    }

    PRINT_DEBUG_LOG("REMAP: vert_num:%d   edge_num = %lld,  in_edge_num = %lld\n", vert_bag_config->data_size, REMAP_edge_num, REMAP_in_edge_num);
    //std::cout<<"REMAP: vert_num: "<<vert_bag_config->data_size<<" edge_num:"<<REMAP_edge_num<<" in_edge_num:"<<REMAP_in_edge_num<<std::endl;

    REMAP_desc_ofstream.open(REMAP_desc_file.c_str());
    REMAP_desc_ofstream << "[description]\n";
    REMAP_desc_ofstream << "min_vertex_id = " << 0 << "\n";
    REMAP_desc_ofstream << "max_vertex_id = " << (vert_bag_config->data_size-1) << "\n";
    REMAP_desc_ofstream << "num_of_edges = " << REMAP_edge_num << "\n";
    //REMAP_desc_ofstream << "max_out_edges = " <<  << "\n";
    if(with_type1)
    {
        REMAP_desc_ofstream << "edge_type = " << 1 << "\n";
    }
    else
    {
        REMAP_desc_ofstream << "edge_type = " << 2 << "\n";
    }
    REMAP_desc_ofstream << "with_in_edge = " << with_in_edge << "\n";
    REMAP_desc_ofstream.close();  


    unmap_vert_remap_file();
    std::cout<<"remap over!"<<std::endl;


    return REMAP_desc_file;

    
}

template<typename VA, typename U, typename T>
std::string Fog_task<VA, U, T>::create_dataset(struct bag_config * vert_bag_config, index_vert_array<T> * vertex_index)
{
    PRINT_DEBUG_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //PRINT_DEBUG_TEST_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //PRINT_DEBUG_CV_LOG("*********************************** task %d CREATE_DATASET****************************************\n", this->get_task_id());
    //lvhuiming debug
    //if(this->get_task_id() == 6)
    //{
    /*
    FILE * debug_out_edge_file;
    FILE * debug_in_edge_file;
    std::string out_edge_file_name = "DEBUG_out_edge_file.txt";
    std::string in_edge_file_name = "DEBUG_in_edge_file.txt";
    debug_out_edge_file = fopen(out_edge_file_name.c_str(), "wt+");
    debug_in_edge_file = fopen(in_edge_file_name.c_str(), "wt+");
    */
    //}
    //debug end

    bool with_type1 = false;
    if(sizeof(T)==sizeof(type1_edge))
    {
        with_type1 = true;
    }
    bool with_in_edge = gen_config.with_in_edge; 

    remap_array_header = (u32_t * )map_vert_remap_file(vert_bag_config->data_name);
    /*
       for(int i = 0 ;i < vert_bag_config->data_size; i++)
       {
       PRINT_DEBUG_CV_LOG("%d\n",remap_array_header[i]);
       }
       */

    struct convert::edge * REMAP_edge_buffer = NULL; 
    struct convert::type2_edge * type2_REMAP_edge_buffer = NULL;
    if(with_type1)
    {
        REMAP_edge_buffer = new struct convert::edge[REMAP_EDGE_BUFFER_LEN];
        memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
    }
    else
    {
        type2_REMAP_edge_buffer = new struct convert::type2_edge[REMAP_EDGE_BUFFER_LEN];
        memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
    }

    struct convert::vert_index * REMAP_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
    memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

    struct convert::in_edge * REMAP_in_edge_buffer = NULL;
    struct convert::vert_index * REMAP_in_vert_buffer = NULL;
    if(with_in_edge)
    {
        REMAP_in_edge_buffer = new struct convert::in_edge[REMAP_EDGE_BUFFER_LEN];
        REMAP_in_vert_buffer = new struct convert::vert_index[REMAP_VERT_BUFFER_LEN];
        memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
        memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
    }
    //std::cout<<"memset ok\n";

    int REMAP_edge_fd = 0;
    int REMAP_index_fd = 0;
    int REMAP_in_edge_fd = 0;
    int REMAP_in_index_fd = 0;
    std::ofstream REMAP_desc_ofstream;

    //std::string temp_file_name = vm["graph"].as<std::string>();
    //temp_file_name = temp_file_name.substr(temp_file_name.find_last_of("/")+1);
    //temp_file_name = temp_file_name.substr(0, temp_file_name.find_last_of("."));
    /*
    std::string temp_file_name = gen_config.graph_path + "/temp";
    //std::cout<<temp_file_name<<std::endl;
    std::stringstream tmp_str_stream;
    tmp_str_stream<<(vert_bag_config->bag_id);
    std::string REMAP_edge_file     = temp_file_name + tmp_str_stream.str() + ".edge"    ;
    std::string REMAP_index_file    = temp_file_name + tmp_str_stream.str() + ".index"   ;
    std::string REMAP_in_edge_file  = temp_file_name + tmp_str_stream.str() + ".in_edge" ;
    std::string REMAP_in_index_file = temp_file_name + tmp_str_stream.str() + ".in_index";
    std::string REMAP_desc_file     = temp_file_name + tmp_str_stream.str() + ".desc"    ; 
    */

    std::string temp_file_name      = vert_bag_config->data_name;
    std::string REMAP_edge_file     = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".edge"    ;
    std::string REMAP_index_file    = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".index"   ;
    std::string REMAP_in_edge_file  = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".in-edge" ;
    std::string REMAP_in_index_file = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".in-index";
    std::string REMAP_desc_file     = temp_file_name.substr(0, temp_file_name.find_last_of(".")) + ".desc"    ; 

    std::cout<<REMAP_edge_file<<std::endl;
    std::cout<<REMAP_index_file<<std::endl;
    std::cout<<REMAP_in_edge_file<<std::endl;
    std::cout<<REMAP_in_index_file<<std::endl;
    std::cout<<REMAP_desc_file<<std::endl;


    REMAP_edge_fd = open(REMAP_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(REMAP_edge_fd == -1)
    {
        printf("Cannot create REMAP_edge_file:%s\nAborted..\n", REMAP_edge_file.c_str());
        exit(-1);
    }
    REMAP_index_fd = open(REMAP_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
    if(REMAP_index_fd == -1)
    {
        printf("Cannot create REMAP_index_file:%s\nAborted..\n", REMAP_index_file.c_str());
        exit(-1);
    }
    if(with_in_edge)
    {
        REMAP_in_edge_fd = open(REMAP_in_edge_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
        if(REMAP_in_edge_fd == -1)
        {
            printf("Cannot create REMAP_in_edge_file:%s\nAborted..\n", REMAP_in_edge_file.c_str());
            exit(-1);
        }
        REMAP_in_index_fd = open(REMAP_in_index_file.c_str(), O_CREAT|O_WRONLY, S_IRUSR);
        if(REMAP_in_index_fd == -1)
        {
            printf("Cannot create REMAP_in_index_file:%s\nAborted..\n", REMAP_in_index_file.c_str());
            exit(-1);
        }
    }

    std::cout<<"open file OK!\n";
    
    //u64_t REMAP_vert_num = 0;
    u32_t REMAP_vert_suffix = 0;
    u32_t REMAP_vert_buffer_offset = 0;
    u64_t REMAP_edge_num = 0;
    u32_t REMAP_edge_suffix = 0;
    u32_t REMAP_edge_buffer_offset = 0;
    u64_t recent_REMAP_edge_num = 0;
    u64_t REMAP_in_edge_num = 0;
    u32_t REMAP_in_edge_suffix = 0;
    u32_t REMAP_in_edge_buffer_offset = 0;
    u64_t recent_REMAP_in_edge_num = 0;

    T temp_out_edge;
    fog::in_edge temp_in_edge;

    u32_t temp_for_degree = 0;
    u32_t temp_for_dst_or_src = 0;

    u32_t old_vert_id = 0;
    u32_t new_vert_id = 0;

    int left = 0;
    int right = vert_bag_config->data_size;

    for (int i = 0; i < vert_bag_config->data_size; i++ )
    {
        //std::cout<<i<<std::endl;
        //REMAP_vert_num++;
        old_vert_id = remap_array_header[i];
        temp_for_degree = vertex_index->num_edges(old_vert_id, OUT_EDGE);
        for(u32_t j = 0; j < temp_for_degree; j++) 
        {
            //temp_out_edge = vertex_index->get_out_edge(old_vert_id, j);
            vertex_index->get_out_edge(old_vert_id, j, temp_out_edge);
            //lvhuiming debug
            /*
            if(this->get_task_id() == 6 && i==vert_bag_config->data_size-2)
            {
                std::cout<<"task 6, vert:"<<remap_array_header[vert_bag_config->data_size-2]<<" outdegree = "<<temp_for_degree<<std::endl;
                temp_for_dst_or_src = temp_out_edge->get_dest_value();
                new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                std::cout<<"old desc: "<<temp_for_dst_or_src<<std::endl;
                std::cout<<"new desc: "<<new_vert_id<<std::endl;
            }
            */
            //debug end
            temp_for_dst_or_src = temp_out_edge.get_dest_value();
            new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
            if(UINT_MAX != new_vert_id)
            {
                //lvhuiming debug
                /*
                if(this->get_task_id() == 6)
                {
                    fprintf(debug_out_edge_file, "%d\t%d\n", old_vert_id, temp_for_dst_or_src);
                }
                */
                //debug end
                REMAP_edge_num++;
                REMAP_edge_suffix = REMAP_edge_num - (REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN);
                if(with_type1)
                {
                    REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id; 
                    REMAP_edge_buffer[REMAP_edge_suffix].edge_weight = temp_out_edge.get_edge_value();
                }
                else
                {
                    type2_REMAP_edge_buffer[REMAP_edge_suffix].dest_vert = new_vert_id;
                }

                if(REMAP_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                {
                    if(with_type1)
                    {
                        flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                        memset((char*)REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::edge));
                    }
                    else
                    {
                        flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                        memset((char*)type2_REMAP_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::type2_edge));
                    }

                    REMAP_edge_buffer_offset++;
                }
            }
        }

        REMAP_vert_suffix = i - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN;
        
        if(REMAP_edge_num != recent_REMAP_edge_num)
        {
            REMAP_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_edge_num+1;
            recent_REMAP_edge_num = REMAP_edge_num;
        }
        /* debug
        if(this->task_id==6)
        {
            PRINT_DEBUG_TEST_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_vert_buffer[REMAP_vert_suffix].offset);
        }
        */

        if(with_in_edge)
        {
            temp_for_degree = vertex_index->num_edges(old_vert_id, IN_EDGE);
            for(u32_t j = 0; j < temp_for_degree; j++)
            {
                //temp_in_edge = vertex_index->get_in_edge(old_vert_id, j);
                vertex_index->get_in_edge(old_vert_id, j, temp_in_edge);
                //lvhuiming debug
                /*
                if(this->get_task_id() == 6 && i==42014)
                {            
                    std::cout<<"task 6, vert:"<<remap_array_header[42014]<<" indegree = "<<temp_for_degree<<std::endl;
                    temp_for_dst_or_src = temp_in_edge->get_src_value();
                    new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                    std::cout<<"old desc: "<<temp_for_dst_or_src<<std::endl;
                    std::cout<<"new desc: "<<new_vert_id<<std::endl;
                }
                */
                //debug end
                temp_for_dst_or_src = temp_in_edge.get_src_value();
                new_vert_id = fog_binary_search(remap_array_header, left, right, temp_for_dst_or_src);
                if(UINT_MAX != new_vert_id)
                {
                    //lvhuiming debug
                    /*
                    if(this->get_task_id() == 6)
                    {
                        fprintf(debug_in_edge_file, "%d\t%d\n", temp_for_dst_or_src, old_vert_id);
                    }
                    */
                    //debug end
                    REMAP_in_edge_num++;
                    REMAP_in_edge_suffix = REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN;
                    REMAP_in_edge_buffer[REMAP_in_edge_suffix].in_vert = new_vert_id;
                    if(REMAP_in_edge_suffix == REMAP_EDGE_BUFFER_LEN-1)
                    {
                        flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                                REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                        memset((char*)REMAP_in_edge_buffer, 0, REMAP_EDGE_BUFFER_LEN*sizeof(convert::in_edge));
                        REMAP_in_edge_buffer_offset++;
                    }
                }
            }
            if(REMAP_in_edge_num != recent_REMAP_in_edge_num)
            {
                REMAP_in_vert_buffer[REMAP_vert_suffix].offset = recent_REMAP_in_edge_num+1;
                recent_REMAP_in_edge_num = REMAP_in_edge_num;
            }
            /* debug
            if(this->task_id==6)
            {
                PRINT_DEBUG_CV_LOG("task_id = %d, vid = %d, offset = %llu\n", this->task_id, i, REMAP_in_vert_buffer[REMAP_vert_suffix].offset);
            }
            */
        }

        if(REMAP_vert_suffix == REMAP_VERT_BUFFER_LEN-1)
        {
            flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
                    REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
            memset((char*)REMAP_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));

            if(with_in_edge)
            {
                flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                        REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
                memset((char*)REMAP_in_vert_buffer, 0, REMAP_VERT_BUFFER_LEN*sizeof(convert::vert_index));
            }

            REMAP_vert_buffer_offset++;
        }
    }

    std::cout<<"for over!\n";


    if(with_type1)
    {
        //flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));
        flush_buffer_to_file(REMAP_edge_fd, (char*)REMAP_edge_buffer,
                (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::edge));

        delete [] REMAP_edge_buffer;
        
    }
    else
    {
        //flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));
        flush_buffer_to_file(REMAP_edge_fd, (char*)type2_REMAP_edge_buffer,
                (1 + REMAP_edge_num - REMAP_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::type2_edge));
        
        delete [] type2_REMAP_edge_buffer;
    }

    flush_buffer_to_file(REMAP_index_fd, (char*)REMAP_vert_buffer,
            (vert_bag_config->data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));
    delete [] REMAP_vert_buffer;
    
    close(REMAP_edge_fd);
    close(REMAP_index_fd);

    if(with_in_edge)
    {
        //flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
        //        (REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
        flush_buffer_to_file(REMAP_in_edge_fd, (char*)REMAP_in_edge_buffer,
                (1 + REMAP_in_edge_num - REMAP_in_edge_buffer_offset*REMAP_EDGE_BUFFER_LEN)*sizeof(convert::in_edge));
        flush_buffer_to_file(REMAP_in_index_fd, (char*)REMAP_in_vert_buffer,
                (vert_bag_config->data_size - REMAP_vert_buffer_offset*REMAP_VERT_BUFFER_LEN)*sizeof(convert::vert_index));

        delete [] REMAP_in_edge_buffer;
        delete [] REMAP_in_vert_buffer;

        close(REMAP_in_edge_fd);
        close(REMAP_in_index_fd);
    }

    PRINT_DEBUG_LOG("REMAP: vert_num:%d   edge_num = %lld,  in_edge_num = %lld\n", vert_bag_config->data_size, REMAP_edge_num, REMAP_in_edge_num);
    //std::cout<<"REMAP: vert_num: "<<vert_bag_config->data_size<<" edge_num:"<<REMAP_edge_num<<" in_edge_num:"<<REMAP_in_edge_num<<std::endl;

    REMAP_desc_ofstream.open(REMAP_desc_file.c_str());
    REMAP_desc_ofstream << "[description]\n";
    REMAP_desc_ofstream << "min_vertex_id = " << 0 << "\n";
    REMAP_desc_ofstream << "max_vertex_id = " << (vert_bag_config->data_size-1) << "\n";
    REMAP_desc_ofstream << "num_of_edges = " << REMAP_edge_num << "\n";
    //REMAP_desc_ofstream << "max_out_edges = " <<  << "\n";
    if(with_type1)
    {
        REMAP_desc_ofstream << "edge_type = " << 1 << "\n";
    }
    else
    {
        REMAP_desc_ofstream << "edge_type = " << 2 << "\n";
    }
    REMAP_desc_ofstream << "with_in_edge = " << with_in_edge << "\n";
    REMAP_desc_ofstream.close();  


    unmap_vert_remap_file();
    std::cout<<"remap over!"<<std::endl;


    return REMAP_desc_file;

    
}

//std::cout<<"last write\n";

template<typename VA, typename U, typename T>
void Fog_task<VA, U, T>::set_alg_ptr(Fog_program<VA, U, T> * alg_ptr)
{
    this->m_task_alg_ptr = alg_ptr;
}


template<typename VA, typename U, typename T>
Fog_program<VA, U, T> * Fog_task<VA, U, T>::get_alg_ptr()
{
    return this->m_task_alg_ptr;
}


template<typename VA, typename U, typename T>
void Fog_task<VA, U, T>::set_task_id(int t_id)
{
    this->task_id = t_id;
}

template<typename VA, typename U, typename T>
int Fog_task<VA, U, T>::get_task_id()
{
    return this->task_id;
}

template<typename VA, typename U, typename T>
void Fog_task<VA, U, T>::set_task_config(struct task_config * in_task_config)
{
    this->m_task_config = in_task_config;
}

template<typename VA, typename U, typename T>
struct task_config * Fog_task<VA, U, T>::get_task_config()
{
    return this->m_task_config;
}

template<typename VA, typename U, typename T>
char * Fog_task<VA, U, T>::map_vert_remap_file(std::string file_name)
{
    struct stat st;
    char * memblock;
    remap_fd = open(file_name.c_str(), O_RDONLY);
    if(remap_fd < 0)
    {
        PRINT_ERROR("cannot open remap file %s\n", file_name.c_str());
        exit(-1);
    }
    fstat(remap_fd, &st);
    remap_file_length = (u64_t)st.st_size;
    memblock = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, remap_fd, 0);
    if(MAP_FAILED == memblock)
    {
        close(remap_fd);
        PRINT_ERROR("remap file %s mapping failed!\n", file_name.c_str());
        exit(-1);
    }
    return memblock;

}

template<typename VA, typename U, typename T>
void Fog_task<VA, U, T>::unmap_vert_remap_file()
{
    if(munmap((void*)remap_array_header, remap_file_length) == -1)
    {
        PRINT_ERROR("unmap file error!\n");
        close(remap_fd);
        exit(-1);
    }
    close(remap_fd);
}

template<typename D>
u32_t fog_binary_search(D * array, int left, int right, D key)
{
    int middle = 0;
    while(left <= right)
    {
        middle = (left+right)/2;
        if(key == array[middle])
        {
            return middle;
        }
        else if(key > array[middle])
        {
            left = middle + 1;
        }
        else
        {
            right = middle - 1;
        }
    }

    return UINT_MAX;
}

template<typename D>
u32_t fog_sequence_search(D * array, int left, int right, D key)
{
    for (int i = left; i <= right; i++)
    {
        if(array[i] == key)
        {
            return i;
        }
    }

    return UINT_MAX;
}
