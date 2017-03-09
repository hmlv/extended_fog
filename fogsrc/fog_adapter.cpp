/*****************************************************************************************************
 * Authors:
 *    Huiming Lv
 *
 * Routines:
 *    base class of the programs
 *
 * Notes:
 *
 *
 * ***************************************************************************************************/


#include <cassert>
#include <fstream>
#include <stdlib.h>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>

#include "fog_adapter.h"
#include "fog_engine.hpp"
#include "print_debug.hpp"
#include "config_parse.h"
#include "config.hpp"
#include "options_utils.h"
#include "index_vert_array.hpp"
#include "bitmap.hpp"

#include <sys/sysinfo.h>
#include <sys/stat.h>

struct general_config gen_config;
FILE * log_file;
FILE * test_log_file;
//FILE * cv_log_file;
boost::program_options::options_description desc;
boost::program_options::variables_map vm;
boost::property_tree::ptree pt;
int TASK_ID;
std::vector<struct bag_config> task_bag_config_vec;
std::queue<int> queue_task_id;
//lvhuiming debug
/*
FILE * debug_out_edge_file;
FILE * debug_in_edge_file;
*/
//debug end

Fog_adapter::Fog_adapter()
{

}

unsigned int Fog_adapter::init(int argc, const char ** argv)
{
    ///////////////////////////////////////////////////////////////
    /*
    std::string out_edge_file_name = "DEBUG_out_edge_file.txt";
    std::string in_edge_file_name = "DEBUG_in_edge_file.txt";
    debug_out_edge_file = fopen(out_edge_file_name.c_str(), "wt+");
    debug_in_edge_file = fopen(in_edge_file_name.c_str(), "wt+");
    */
    /////////////////////////
    std::string user_command;
    for(int i = 0; i < argc; i++)
    {
        user_command += argv[i];
        user_command += " ";
    }

    std::string prog_name_app;
    std::string desc_name;
    std::string log_file_name;
    std::string test_log_file_name;
    //std::string cv_log_file_name;

    setup_options_fog(argc, argv);
    //prog_name_app = vm["application"].as<std::string>();
    std::string prog_name = argv[0];
    prog_name_app = prog_name.substr(prog_name.find_last_of("/")+1);
    desc_name = vm["graph"].as<std::string>();

    time_t timep;
    time(&timep);
    struct tm *tm_p = localtime(&timep);
    char temp[100];
    sprintf(temp, "%d.%d.%d-%d:%d:%d", tm_p->tm_year+1900, tm_p->tm_mon+1,
            tm_p->tm_mday, tm_p->tm_hour, tm_p->tm_min, tm_p->tm_sec);

    log_file_name = "print-" + prog_name_app + "-" + std::string(temp) + "-.log";
    test_log_file_name = "test-" + prog_name_app + "-" + std::string(temp) + "-.Log";
    //cv_log_file_name = "cv-" + prog_name_app + "-" + std::string(temp) + "-.Log";

    init_graph_desc(desc_name);

    //config subjected to change.
    gen_config.num_processors = vm["processors"].as<unsigned long>();
    gen_config.num_io_threads = vm["diskthreads"].as<unsigned long>();
    //the unit of memory is MB
    gen_config.memory_size = (u64_t)vm["memory"].as<unsigned long>()*1024*1024;
    gen_config.origin_mem_size = gen_config.memory_size;

    //std::cout << "sizeof type1_edge = " << sizeof(type1_edge) << std::endl;
    //std::cout << "sizeof type2_edge = " << sizeof(type2_edge) << std::endl;
    //add by  hejian
    if (!(log_file = fopen(log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", log_file_name.c_str());
        exit(666);
    }
    if (!(test_log_file = fopen(test_log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", test_log_file_name.c_str());
        exit(666);
    }
    /*
    if (!(cv_log_file = fopen(cv_log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", cv_log_file_name.c_str());
        exit(666);
    }
    */
    PRINT_DEBUG("Your command is: %s\n", user_command.c_str());

    gen_config.min_vert_id = pt.get<u32_t>("description.min_vertex_id");
    gen_config.max_vert_id = pt.get<u32_t>("description.max_vertex_id");
    gen_config.num_edges = pt.get<u64_t>("description.num_of_edges");
    gen_config.max_out_edges = pt.get<u32_t>("description.max_out_edges");
    gen_config.graph_path = desc_name.substr(0, desc_name.find_last_of("/"));
    gen_config.vert_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".index";
    gen_config.edge_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".edge";
    gen_config.attr_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".attr";

    unsigned int type1_or_type2 = pt.get<u32_t>("description.edge_type");
    bool with_inedge = pt.get<bool>("description.with_in_edge");
    bool i_in_edge = vm["in-edge"].as<bool>();
    //bool i_in_edge = true;
    if(i_in_edge)
    {
        if(with_inedge)
        {
            gen_config.with_in_edge = true;
            gen_config.in_edge_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-edge";
            gen_config.in_vert_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-index";
        }
        else
        {
            PRINT_ERROR("in_edge file doesn't exit!\n");
        }
    }

    PRINT_DEBUG( "Graph name: %s\nApplication name:%s\n", desc_name.c_str(), prog_name_app.c_str() );
    PRINT_DEBUG( "Configurations:\n" );
    PRINT_DEBUG( "gen_config.memory_size = 0x%llx\n", gen_config.memory_size );
    PRINT_DEBUG( "gen_config.min_vert_id = %d\n", gen_config.min_vert_id );
    PRINT_DEBUG( "gen_config.max_vert_id = %d\n", gen_config.max_vert_id );
    PRINT_DEBUG( "gen_config.num_edges = %lld\n", gen_config.num_edges );
    PRINT_DEBUG( "gen_config.vert_file_name = %s\n", gen_config.vert_file_name.c_str() );
    PRINT_DEBUG( "gen_config.edge_file_name = %s\n", gen_config.edge_file_name.c_str() );
    PRINT_DEBUG( "gen_config.attr_file_name(WRITE ONLY) = %s\n", gen_config.attr_file_name.c_str() );

    return type1_or_type2;
}


/*
 * this function will flush the content of buffer to file, fd
 * the length of the buffer should be "size".
 * Returns: -1 means failure
 * on success, will return the number of bytes that are actually
 * written.
 */
int flush_buffer_to_file( int fd, char* buffer, unsigned int size )
{
    unsigned int n, offset, remaining, res;
    n = offset = 0;
    remaining = size;
    while(n<size){
        res = write( fd, buffer+offset, remaining);
        n += res;
        offset += res;
        remaining = size-n;
    }
    return n;
}

struct mmap_config mmap_file(std::string file_name)
{
    struct mmap_config m_config;
    struct stat st;
    m_config.fd = open(file_name.c_str(), O_RDONLY);
    if(m_config.fd < 0)
    {
        PRINT_ERROR("cannot openm attr file %s\n", file_name.c_str());
        exit(-1);
    }
    fstat(m_config.fd, &st);
    m_config.file_length = (u64_t)st.st_size;
    m_config.mmap_head = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, m_config.fd, 0);
    if(MAP_FAILED == m_config.mmap_head)
    {
        close(m_config.fd);
        PRINT_ERROR("remap file %s mapping failed\n", file_name.c_str());
        exit(-1);
    }

    return m_config;
}

void unmap_file(const struct mmap_config & m_config)
{
    if(munmap((void*)m_config.mmap_head, m_config.file_length) == -1)
    {
        PRINT_ERROR("unmap file error!\n");
        close(m_config.fd);
        exit(-1);
    }
    close(m_config.fd);
}

u64_t get_file_size(const std::string file)
{
    struct stat stat_buf;
    if(stat(file.c_str(), &stat_buf) < 0)
    {
        PRINT_ERROR("get %s size failed\n", file.c_str());
        //return -1;
    }

    return stat_buf.st_size;
}

bool in_mem(struct task_config * t_config, size_t attr_size)
{
    struct sysinfo sysi;
    u64_t require_size = 0;

    sysinfo(&sysi);
    //edge+index
    require_size += get_file_size(t_config->edge_file_name);
    require_size += get_file_size(t_config->vert_file_name);
    require_size += get_file_size(t_config->in_edge_file_name);
    require_size += get_file_size(t_config->in_vert_file_name);

    require_size = require_size + ((t_config->max_vert_id+1) * attr_size * 3);

    //std::cout<<require_size<<std::endl;
    //std::cout<<sysi.totalram<<std::endl;

    if(require_size < sysi.totalram)
    {
        return true;
    }

    return false;
}
