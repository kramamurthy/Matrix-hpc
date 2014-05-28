#include "matrix_server.h"
#include <time.h>

Worker::Worker() {
	
}

Worker::~Worker() {

}

TaskQueue wqueue;
TaskQueue rqueue;
TaskQueue mqueue;
CompQueue cqueue;
static pthread_mutex_t w_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "wait queue"
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;                // Lock for the "ready queue"
static pthread_mutex_t c_lock = PTHREAD_MUTEX_INITIALIZER;                // Lock for the "complete queue"
static pthread_mutex_t m_lock = PTHREAD_MUTEX_INITIALIZER;              // Lock for the "migrate queue"
static pthread_mutex_t mutex_idle = PTHREAD_MUTEX_INITIALIZER;          // Lock for the "num_idle_cores"
static pthread_mutex_t mutex_finish = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "finish file"
static pthread_mutex_t count_threads=PTHREAD_MUTEX_INITIALIZER;			// Lock for the count on threads running HPC tasks.
pthread_barrier_t hpc_barrier;
pthread_mutex_t completed_hpc_map = PTHREAD_MUTEX_INITIALIZER;
std::map<std::string,int> completed_hpc_job_map;

static pthread_mutex_t zht_lock = PTHREAD_MUTEX_INITIALIZER;        // Lock for the "zht"

//queue<string*> insertq;
queue<string*> waitq;
queue<string*> insertq_new;
queue<int*> migrateq;
bitvec migratev;
queue<string*> notifyq;

//pthread_mutex_t iq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t waitq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t iq_new_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t notq_lock = PTHREAD_MUTEX_INITIALIZER;


struct package_thread_args {
	queue<string*> *source;
	TaskQueue *dest;
	pthread_mutex_t *slock;
	pthread_mutex_t *dlock;
	Worker *worker;
};

static pthread_mutex_t msg_lock = PTHREAD_MUTEX_INITIALIZER;

/*
TaskQueue* ready_queue;		// Queue of tasks which are ready to run
TaskQueue* migrate_queue;       // Queue of tasks which are going to migrate
TaskQueue* wait_queue;		// Queue of tasks which are waiting for the finish of dependent tasks
TaskQueue* running_queue;	// Queue of tasks which are being executed
*/

ofstream fin_fp;
ofstream log_fp;
long msg_count[10];

int ON = 1;
long task_comp_count = 0;

uint32_t nots = 0, notr = 0;

long start_poll = 1000; long start_thresh = 1000000;
long end_poll = 100000;	long end_thresh = 10000000;
int diff_thresh = 500;
timespec poll_start, poll_end;
uint64_t failed_attempts = 0;
uint64_t fail_threshold = 1000;
int work_steal_signal = 1;

//Worker *worker;
static NoVoHT *pmap;

pthread_attr_t attr; // thread attribute
static ofstream worker_start;
static ofstream task_fp;
static ofstream load_fp;
static ofstream migrate_fp;

/* filenames */
static string file_worker_start;	
static string file_task_fp;			
static string file_migrate_fp;		
static string file_fin_fp;			
static string file_log_fp;		

static string loadstr, taskstr, idlestr,releasestr;

Worker::Worker(char *parameters[], NoVoHT *novoht) {
	/* set thread detachstate attribute to DETACHED */
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_attr_setscope(&attr,PTHREAD_SCOPE_SYSTEM);
	/* filename definitions */
	set_dir(parameters[9],parameters[10]);
	file_worker_start.append(shared);	file_worker_start.append("startinfo");
	file_task_fp.append(prefix);		file_task_fp.append("pkgs");
	file_migrate_fp.append(prefix);		file_migrate_fp.append("log_migrate");
	file_fin_fp.append(prefix);		file_fin_fp.append("finish");
	file_log_fp.append(prefix);		file_log_fp.append("log_worker");
	
	pmap = novoht;
	Env_var::set_env_var(parameters);
	svrclient.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
	//svrzht.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
	//svrmig.initialize(Env_var::cfgFile, Env_var::membershipFile, Env_var::TCP);
	
	//Calls set_ip method in the matrix_util to get the hostname of the system
	if(set_ip(ip)) {
		printf("Could not get the IP address of this machine!\n");
		exit(1);
	}
	
	for(int i = 0; i < 10; i++) {
		msg_count[i] = 0;
	}
	
	poll_interval = start_poll;
	poll_threshold = start_thresh;
	//Gets the number of nodes - The number of entries in the neighbor file
	num_nodes = svrclient.memberList.size();
	cout << "The number of workers available is:" << num_nodes << endl;
	//Gets the number of cores in the system
	num_cores = 2;//sysconf(_SC_NPROCESSORS_ONLN);
	cout << "The number of cores on the system is" << num_cores << endl;
	//Assign num_cores to num_idle_cores
	num_idle_cores = num_cores;
	//neigh_mode = 'd';
	//worker.num_neigh = (int)(sqrt(worker.num_nodes));
	num_neigh = (int)(log(num_nodes)/log(2));
	cout << "The value of num_neigh is " << num_neigh << endl;
	neigh_index = new int[num_neigh];
	selfIndex = getSelfIndex(ip, atoi(parameters[1]), svrclient.memberList);	// replace "localhost" with proper hostname, host is the IP in C++ string
	cout << "The self index of this server is " << selfIndex << endl;
	ostringstream oss;
        oss << selfIndex;
	
	string f1 = file_fin_fp;
	f1 = f1 + oss.str();
	fin_fp.open(f1.c_str(), ios_base::app);

	if(LOGGING) {

		string f2 = file_task_fp;
		f2 = f2 + oss.str();
		task_fp.open(f2.c_str(), ios_base::app);

		string f3 = file_log_fp;
		f3 = f3 + oss.str();
		log_fp.open(f3.c_str(), ios_base::app);
		
		string f4 = file_migrate_fp;
		f4 = f4 + oss.str();
		migrate_fp.open(f4.c_str(), ios_base::app);
	}
	
	migratev = bitvec(num_nodes);
	
	Package loadPackage, tasksPackage, idlePackage,releaseResourcePackage;
	string loadmessage("Load Information!");
	loadPackage.set_virtualpath(loadmessage);
	loadPackage.set_operation(13);
	loadstr = loadPackage.SerializeAsString();
	
	stringstream selfIndexstream;
	selfIndexstream << selfIndex;
	string taskmessage(selfIndexstream.str());
	tasksPackage.set_virtualpath(taskmessage);
	tasksPackage.set_operation(14);
	taskstr = tasksPackage.SerializeAsString();
	
	string coremessage("Core Information");
	idlePackage.set_virtualpath(coremessage);
	idlePackage.set_operation(16);
	idlestr = idlePackage.SerializeAsString();
	
	string releasemessage("Release Resources");
	releaseResourcePackage.set_virtualpath(releasemessage);
	releaseResourcePackage.set_operation(18);
	releasestr = releaseResourcePackage.SerializeAsString();
	
	srand((selfIndex+1)*(selfIndex+5));
	int rand_wait = rand() % 20;
	cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << endl;
	//cout << "Worker ip = " << ip << " selfIndex = " << selfIndex << " going to wait for " << rand_wait << " seconds" << endl;
	sleep(rand_wait);

	file_worker_start.append(oss.str());
        string cmd("touch ");
        cmd.append(file_worker_start);
        executeShell(cmd);
        
        worker_start.open(file_worker_start.c_str(), ios_base::app);
        if(worker_start.is_open()) {
        	worker_start << ip << ":" << selfIndex << " " << std::flush;
        	worker_start.close();
        	worker_start.open(file_worker_start.c_str(), ios_base::app);
        }

        clock_gettime(CLOCK_REALTIME, &poll_start);

	int err;
	/*pthread_t *ready_queue_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
	pthread_create(ready_queue_thread, &attr, check_ready_queue, NULL);*/
	try {
	//Ready queue thread
	pthread_t *ready_queue_thread = new pthread_t[2];
	//Restricting the worker to 2 threads
	for(int i = 0; i < 2; i++) {
		err = pthread_create(&ready_queue_thread[i], &attr, check_ready_queue, (void*) this);
		if(err){
                	printf("work_steal_init: pthread_create: ready_queue_thread: %s\n", strerror(errno));
                        exit(1);
                }
	}

	pthread_t *ready_queue_thread_migrate_task = new pthread_t();
	pthread_create(ready_queue_thread_migrate_task, &attr, check_ready_queue_for_migrated_tasks, (void*) this);

	pthread_t *wait_queue_thread = new pthread_t();
	err = pthread_create(wait_queue_thread, &attr, check_wait_queue, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: wait_queue_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *complete_queue_thread = new pthread_t();
        err = pthread_create(complete_queue_thread, &attr, check_complete_queue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: complete_queue_thread: %s\n", strerror(errno));
                exit(1);
        }
	
	package_thread_args rq_args, wq_args;
	rq_args.source = &insertq_new;	wq_args.source = &waitq;
	rq_args.dest = &rqueue;		wq_args.dest = &wqueue;
	rq_args.slock = &iq_new_lock;	wq_args.slock = &waitq_lock;
	rq_args.dlock = &lock;		wq_args.dlock = &w_lock;	
	rq_args.worker = this;		wq_args.worker = this;
	pthread_t *waitq_thread = new pthread_t();
	err = pthread_create(waitq_thread, &attr, HB_insertQ_new, (void*) &wq_args);
	if(err){
                printf("work_steal_init: pthread_create: waitq_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *readyq_thread = new pthread_t();
        err = pthread_create(readyq_thread, &attr, HB_insertQ_new, (void*) &rq_args);
        if(err){
                printf("work_steal_init: pthread_create: ready_thread: %s\n", strerror(errno));
                exit(1);
        }
	
	pthread_t *migrateq_thread = new pthread_t();
        err = pthread_create(migrateq_thread, &attr, migrateTasks, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: migrateq_thread: %s\n", strerror(errno));
                exit(1);
        }

	pthread_t *notq_thread = new pthread_t();
        err = pthread_create(notq_thread, &attr, notQueue, (void*) this);
        if(err){
                printf("work_steal_init: pthread_create: notq_thread: %s\n", strerror(errno));
                exit(1);
        }

	int min_lines = svrclient.memberList.size();
	//min_lines++;	
        string filename(file_worker_start);
        //string cmd("wc -l ");	
        //cmd = cmd + filename + " | awk {\'print $1\'}";
	
	string cmd2("ls -l "); 	cmd2.append(shared);	cmd2.append("startinfo*");	cmd2.append(" | wc -l");
        string result = executeShell(cmd2);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
        while(atoi(result.c_str()) < min_lines) {
		sleep(2);
		 //cout << "server: " << worker.selfIndex << " minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
                result = executeShell(cmd2);
        }
	//cout << "server: " << selfIndex << " minlines = " << min_lines << " cmd = " << cmd2 << " result = " << result << endl;
	
	/*int num = min_lines - 1;
        stringstream num_ss;
        num_ss << num;
        //min_lines++;
	string cmd1("cat ");    cmd1.append(shared);    cmd1.append("startinfo"); 	cmd1.append(num_ss.str());     cmd1.append(" | wc -l");
	string result1 = executeShell(cmd1);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	while(atoi(result1.c_str()) < 1) {
		sleep(2);
		result1 = executeShell(cmd1);
	}
	cout << "worksteal started: server: " << selfIndex << " minlines = " << 1 << " cmd = " << cmd1 << " result = " << result1 << endl;*/
	
    pthread_t *work_steal_thread = new pthread_t();//(pthread_t*)malloc(sizeof(pthread_t));
	err = pthread_create(work_steal_thread, &attr, worksteal, (void*) this);
	if(err){
                printf("work_steal_init: pthread_create: work_steal_thread: %s\n", strerror(errno));
        	exit(1);
        }

	delete ready_queue_thread;
	delete wait_queue_thread;
	delete complete_queue_thread;
	delete work_steal_thread;
	delete readyq_thread;
	delete waitq_thread;
	delete migrateq_thread;
	delete notq_thread;
	}
	catch (std::bad_alloc& exc) {
		cout << "work_steal_init: failed to allocate memory while creating threads" << endl;
		exit(1);
	}
}

int Worker::zht_ins_mul(Package &package) {
        int num_vector_count, per_vector_count;
        vector< vector<string> > tokenize_string = tokenize(package.realfullpath(), '$', '#', num_vector_count, per_vector_count);
	//cout << " num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
        for(int i = 0; i < per_vector_count; i++) {
                zht_insert(tokenize_string.at(0).at(i));
        } //cout << "Server: " << selfIndex << " no. tasks inserted = " << per_vector_count << endl;
        return per_vector_count;
}

// parse the input string containing two delimiters
vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count) {
	vector< vector<string> > token_vector;
	stringstream whole_stream(input);
	num_vector = 0; per_vector_count = 0;
	string perstring;
	while(getline(whole_stream, perstring, delim1)) { //cout << "pertask = " << pertask << endl;
		num_vector++;
                vector<string> per_vector;
                size_t prev = 0, pos;
		while ((pos = perstring.find_first_of(delim2, prev)) != string::npos) {
                       	if (pos > prev) {
				try {
                               		per_vector.push_back(perstring.substr(prev, pos-prev));
				}
				catch (exception& e) {
					cout << "tokenize: (prev, pos-prev) " << " " << e.what() << endl;
					exit(1);
				}
                       	}
              		prev = pos+1;
               	}
               	if (prev < perstring.length()) {
			try {
                       		per_vector.push_back(perstring.substr(prev, string::npos));
			}
			catch (exception& e) {
                       	        cout << "tokenize: (prev, string::npos) " << " " << e.what() << endl;
                               	exit(1);
                        }
               	}
		try {
			token_vector.push_back(per_vector);
		}
		catch (exception& e) {
                        cout << "tokenize: token_vector.push_back " << " " << e.what() << endl;
                	exit(1);
                }
	}
	if(token_vector.size() > 0) {
		per_vector_count = token_vector.at(0).size();
	}
	return token_vector;
}

void* notQueue(void* args) {
	Worker *worker = (Worker*)args;
	string *st;
	Package package;

	while(ON) {
		while(notifyq.size() > 0) {
			pthread_mutex_lock(&notq_lock);
			if(notifyq.size() > 0) {
				try {
				st = notifyq.front();
				notifyq.pop();
				}
				catch (exception& e) {
					cout << "void* notifyq: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(&notq_lock);
                                continue;
                        }
			pthread_mutex_unlock(&notq_lock);
			package.ParseFromString(*st);
			delete st;
			worker->update(package);
		}
	}
}

// Insert tasks into queue without repeated fields
//int32_t Worker::HB_insertQ_new(NoVoHT *map, Package &package) {
void* HB_insertQ_new(void* args) {

	package_thread_args targs = *((package_thread_args*)args);
	queue<string*> *source = targs.source;
	TaskQueue *dest = targs.dest;
	pthread_mutex_t *slock = targs.slock;
	pthread_mutex_t *dlock = targs.dlock;
	Worker *worker = targs.worker;

	string *st;
	Package package;

	while(ON) {
		while(source->size() > 0) {
			pthread_mutex_lock(slock);
			if(source->size() > 0) {
				try {
				st = source->front();
				source->pop(); //cout << "recd something" << endl;
				}
				catch (exception& e) {
					cout << "void* HB_insertQ_new: " << e.what() << endl;
					exit(1);
				}
			}
			else {
                                pthread_mutex_unlock(slock);
                                continue;
                        }
			pthread_mutex_unlock(slock);
			package.ParseFromString(*st);
			delete st;

		        TaskQueue_Item *qi;

			uint32_t task_recd_count = 0;
		        string alltasks(package.realfullpath());
			int num_vector_count, per_vector_count;
		        vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
			//cout << "num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
			task_recd_count = num_vector_count;
			for(int i = 0; i < num_vector_count; i++) {
				qi = new TaskQueue_Item();
				try {
                			qi->task_id = tokenize_string.at(i).at(0); //cout << "insertq: qi->task_id = " << qi->task_id << endl;
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                                	exit(1);
                                }
				/*stringstream num_moves_ss;
				try {
			                num_moves_ss << tokenize_string.at(i).at(1);
				}
				catch (exception& e) {
                                        cout << "void* HB_insertQ_new: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                                        exit(1);
                                }
                		num_moves_ss >> qi->num_moves;*/

                		if(LOGGING) {
                                	task_fp << " taskid = " << qi->task_id;
                                	//task_fp << " num moves = " << qi->num_moves;
                		}
				
				pthread_mutex_lock(dlock);
				try {
		                        dest->push_back(qi);
                		}
		                catch (std::bad_alloc& exc) {
                		        cout << "HB_InsertQ_new: cannot allocate memory while adding element to ready queue" << endl;
		                        pthread_exit(NULL);
                		}
		                pthread_mutex_unlock(dlock);
		        }
			if(LOGGING) {
				log_fp << "Num tasks received = " << task_recd_count << " Queue length = " << dest->size() << endl;
			}
		}
	}
}


map<uint32_t, NodeList> Worker::get_map(TaskQueue &mqueue) {
	map<uint32_t, NodeList> update_map;
	/*Package package;
	package.set_operation(operation);
	if(operation == 25) {
		package.set_currnode(toid);
	}*/
	uint32_t num_nodes = svrclient.memberList.size();
	for(TaskQueue::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
		uint32_t serverid = myhash(((*it)->task_id).c_str(), num_nodes);
		string str((*it)->task_id); str.append("\'");
		if(update_map.find(serverid) == update_map.end()) {
			str.append("\"");
			NodeList new_list;
			new_list.push_back(str);
			update_map.insert(make_pair(serverid, new_list));
		}
		else {
			NodeList &exist_list = update_map[serverid];
			string last_str(exist_list.back());
			if((last_str.size() + str.size()) > STRING_THRESHOLD) {
				str.append("\"");
				exist_list.push_back(str);
			}
			else {
				exist_list.pop_back();
				str.append(last_str);
				exist_list.push_back(str);
			}
		}
	}
	return update_map;
}

map<uint32_t, NodeList> Worker::get_map(vector<string> &mqueue) {
        map<uint32_t, NodeList> update_map;
        /*Package package;
        package.set_operation(operation);
        if(operation == 25) {
                package.set_currnode(toid);
        }*/
        uint32_t num_nodes = svrclient.memberList.size();
        for(vector<string>::iterator it = mqueue.begin(); it != mqueue.end(); ++it) {
                uint32_t serverid = myhash((*it).c_str(), num_nodes);
                string str(*it); str.append("\'");
                if(update_map.find(serverid) == update_map.end()) {
                        str.append("\"");
                        NodeList new_list;
                        new_list.push_back(str);
                        update_map.insert(make_pair(serverid, new_list));
                }
                else {
                        NodeList &exist_list = update_map[serverid];
                        string last_str(exist_list.back());
                        if((last_str.size() + str.size()) > STRING_THRESHOLD) {
                                str.append("\"");
                                exist_list.push_back(str);
                        }
                        else {
                                exist_list.pop_back();
                                str.append(last_str);
                                exist_list.push_back(str);
                        }
                }
        }
        return update_map;
}

// pack the jobs into multiple packages - 2000 jobs per package
// and insert it into the ready queue of server that requested to steal tasks
//int Worker::migrateTasks(int num_tasks, ZHTClient &clientRet, int index){
void* migrateTasks(void *args) {

	Worker *worker = (Worker*)args;
	int index;
    while(ON) {
        //while(migrateq.size() > 0) {
	while(migratev.any()) {
			pthread_mutex_lock(&mq_lock);
			if(migratev.any()) {
				//int *index = (int*)args;                
                		//index = migrateq.front();
				index = migratev.pop();
                		//migrateq.pop();
				//cout << "1 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			}		        
			else {
				//cout << "migratev count = " << migratev.count() << endl;
				pthread_mutex_unlock(&mq_lock);
				continue;
			}
			if(index < 0 || index >= worker->num_nodes) {
				//cout << "bad index: worker = " << worker->selfIndex << " to index = " << index << endl;
                                pthread_mutex_unlock(&mq_lock);
                        	continue;
                        }
			pthread_mutex_unlock(&mq_lock);
			//cout << "2 worker = " << worker->selfIndex << " to index = " << index << " size = " << rqueue.size() << endl;
			pthread_mutex_lock (&m_lock); pthread_mutex_lock (&lock);
			int32_t num_tasks = rqueue.size()/2;
			if(num_tasks < 1) {
				pthread_mutex_unlock (&lock); pthread_mutex_unlock (&m_lock);
				continue;
			}
			try {	//cout << "going to send " << num_tasks << " tasks" << endl;
				mqueue.assign(rqueue.end()-num_tasks, rqueue.end());
				rqueue.erase(rqueue.end()-num_tasks, rqueue.end());
				//cout << "rqueue size = " << rqueue.size() << " mqueue size = " << mqueue.size() << endl;
			}
			catch (...) {
				cout << "migrateTasks: cannot allocate memory while copying tasks to migrate queue" << endl;
				pthread_exit(NULL);
			}
			pthread_mutex_unlock (&lock);

			map<uint32_t, NodeList> update_map = worker->get_map(mqueue);
			cout << "Calling zht_update " << endl;
			int update_ret = worker->zht_update(update_map, "nodehistory", index);
			/*if(index == worker->selfIndex) {
				cout << "ALERT: MIGRATING TO ITSELF" << endl;
			}*/
			int num_packages = 0;
			long total_submitted = 0;

			num_tasks = mqueue.size();
			while (total_submitted != num_tasks) {
				Package package; string alltasks;
				package.set_virtualpath(worker->ip);
				package.set_operation(22);
				num_packages++;
				int num_tasks_this_package = max_tasks_per_package;
				int num_tasks_left = num_tasks - total_submitted;
				if (num_tasks_left < max_tasks_per_package) {
                	 	       num_tasks_this_package = num_tasks_left;
				}
				for(int j = 0; j < num_tasks_this_package; j++) {
		                //TaskQueue_item* qi = migrate_queue->remove_element();
                			if(mqueue.size() < 1) {
		                	        if(j > 0) {
                			        	total_submitted = total_submitted + j;
							package.set_realfullpath(alltasks);
			                	        string str = package.SerializeAsString();
							pthread_mutex_lock(&msg_lock);
        	        		        	int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index);
							pthread_mutex_unlock(&msg_lock);
	        	                	}
        	        	        	//pthread_mutex_unlock (&m_lock);
	                	        	//return total_submitted;
		                        	//pthread_exit(NULL);
						total_submitted = num_tasks;
						break;
        	            		}
                			try {
						alltasks.append(mqueue.front()->task_id); alltasks.append("\'\""); // Task ID
						/*stringstream num_moves_ss;
			                        num_moves_ss << (mqueue.front()->num_moves + 1);
						alltasks.append(num_moves_ss.str());  alltasks.append("\'\""); // Number of moves*/
		
                			        if(LOGGING) {
				            		migrate_fp << " taskid = " << mqueue.front()->task_id;
			                    		//migrate_fp << " num moves = " << (mqueue.front()->num_moves + 1);
                    		   		}
						delete mqueue.front();
		                   		mqueue.pop_front();
                	    		}
                    	    		catch (...) {
		                    		cout << "migrateTasks: Exception occurred while processing mqueue" << endl;
                	    		}
				}
				if(total_submitted == num_tasks) {
					break;
				}
				total_submitted = total_submitted + num_tasks_this_package;
				package.set_realfullpath(alltasks);
				string str = package.SerializeAsString(); //cout << "r1: " << total_submitted << " tasks" << endl;
				pthread_mutex_lock(&msg_lock);
				int32_t ret = worker->svrclient.svrtosvr(str, str.length(), index); //cout << "r1 sent" << endl;
				pthread_mutex_unlock(&msg_lock);
			}
			pthread_mutex_unlock (&m_lock);
			//cout << "matrix_server: No. of tasks sent = " << total_submitted << endl;
		}
	}
}

//request the worker with given index to send tasks
int32_t Worker::recv_tasks() {
//cout << " 3";

	int32_t num_task;
	pthread_mutex_lock(&msg_lock);
	num_task = svrclient.svrtosvr(taskstr, taskstr.length(), max_loaded_node);
	pthread_mutex_unlock(&msg_lock); 
	/*if(LOGGING) {
		log_fp << "expected num tasks = " << num_task << endl;
	}*/

	clock_gettime(CLOCK_REALTIME, &poll_end);
        timespec diff = timediff(poll_start, poll_end);
	if(diff.tv_sec > diff_thresh) {
		poll_threshold = end_thresh;
	}

	if(num_task > 0) { // there are tasks to receive
		if(diff.tv_sec > diff_thresh) {
			poll_interval = end_poll; // reset the poll interval to 1ms
		}
		else {
			poll_interval = start_poll;
		}
		//cout << "Worker::recv_tasks num_task = " << num_task << endl;
		return num_task;
	}

	else {
		// no tasks
		if(poll_interval < poll_threshold) { // increase poll interval only if it is under 1 sec
			poll_interval *= 2;
		}
		return 0;
	}
}

/*
 * Find the neighbor which has the heaviest load
 */
int32_t Worker::get_max_load()
{	//cout << " in max load " << endl;
	//max_load_struct* new_max_load;
	int i;
	int32_t max_load = -1000000, load;
	
	for(i = 0; i < num_neigh; i++)
	{	//cout << " max load i = " << i << " index = " << neigh_index[i] << endl;
		// Get Load information
		pthread_mutex_lock(&msg_lock);
		load = svrclient.svrtosvr(loadstr, loadstr.length(), neigh_index[i]);
		pthread_mutex_unlock(&msg_lock);
		//cout << "worker = " << selfIndex << " Load = " << load << endl;
		if(load > max_load)
		{
			max_load = load;
			max_loaded_node = neigh_index[i];
		}
	}

	return max_load;
}

/*
 * Randomly choose neighbors dynamically
 */
void Worker::choose_neigh()
{	
	//time_t t;
	//srand((unsigned)time(&t));
	srand(time(NULL));
	int i, ran;
	int x;
	char *flag = new char[num_nodes];
	//cout << "num nodes = " << num_nodes << " num neigh = " << num_neigh << endl;
	/*for(i = 0; i < num_nodes; i++)
	{
		flag[i] = '0';
	}*/
	memset(flag, '0', num_nodes);
	//cout << "flag array set" << endl;
	for(i = 0; i < num_neigh; i++)
	{
		//cout << " i = " << i << endl;
		ran = rand() % num_nodes;		
		x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
		//cout << "ran a " << ran << " " << selfIndex << " " << x << endl;
		//while(flag[ran] == '1' || !strcmp(all_ips[ran], ip))
		while(flag[ran] == '1' || !x)
		{
			ran = rand() % num_nodes;
			x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
			//cout << "ran = " << ran << " x = " << x << endl;
		}
		flag[ran] = '1';
		//strcpy(neigh_ips[i], all_ips[ran]);
		//cout << "i = " << i << " ran = " << ran << endl;
		neigh_index[i] = ran;
		//cout << "ran b " << ran << " " << selfIndex << " " << x << endl;
	}
	//cout << "neigh index set" << endl;
	delete flag;
}

/*
 * Steal tasks from the neighbors
 */
int32_t Worker::steal_task()
{
	
	choose_neigh();
	//cout << "Done choose_neigh" << endl;
	int32_t max_load = get_max_load();

	// While all neighbors have no more available tasks,
	// double the "poll_interval" to steal again
	while(max_load <= 0 && work_steal_signal)
	{
		//cout << "workerid = " << selfIndex << " load = " << max_load << " failed attempts = " << failed_attempts << endl;
		usleep(poll_interval);	failed_attempts++;
		clock_gettime(CLOCK_REALTIME, &poll_end);
        	timespec diff = timediff(poll_start, poll_end);
        	if(diff.tv_sec > diff_thresh) {
                	poll_threshold = end_thresh;
        	}

		if(poll_interval < poll_threshold) { // increase poll interval only if it is under 1 sec
			poll_interval *= 2;
		}

		if(failed_attempts >= fail_threshold) {
			work_steal_signal = 0;
			//cout << worker.selfIndex << " stopping worksteal" << endl;
			return 0;
		}
		/*if(worker.poll_interval == 1024000) {
			return 2;
		}*/
		
		choose_neigh();
		max_load = get_max_load();
		/*if(LOGGING) {
                	log_fp << "Max loaded node = " << max_loaded_node << endl;
        	}*/
	}
	/*if(LOGGING) {
		log_fp << "Max loaded node = " << max_loaded_node << endl;
	}*/
	failed_attempts = 0;
	
	return recv_tasks();
}

// thread to steal tasks it ready queue length becomes zero
void* worksteal(void* args){
	//cout << "entered worksteal thread" << endl;
	Worker *worker = (Worker*)args;

	int num = worker->svrclient.memberList.size() - 1;
        stringstream num_ss;
        num_ss << num;
        //min_lines++;
	string cmd1("cat ");    cmd1.append(shared);    cmd1.append("startinfo"); 	cmd1.append(num_ss.str());     cmd1.append(" | wc -l");
	string result1 = executeShell(cmd1);
	//cout << "server: minlines = " << min_lines << " cmd = " << cmd << " result = " << result << endl;
	while(atoi(result1.c_str()) < 1) {
		sleep(2);
		result1 = executeShell(cmd1);
	}
	//cout << "worksteal started: server: " << worker->selfIndex << " minlines = " << 1 << " cmd = " << cmd1 << " result = " << result1 << endl;
	
	while(work_steal_signal) {
		//while(ready_queue->get_length() > 0) { }
		while(rqueue.size() > 0) { }
		
		// If there are no waiting ready tasks, do work stealing
		//if (worker.num_nodes > 1 && ready_queue->get_length() < 1)
		if (worker->num_nodes > 1 && rqueue.size() < 1)
		{
			int32_t success = worker->steal_task();			
			// Do work stealing until succeed
			while(success == 0)
			{
				failed_attempts++;
				if(failed_attempts >= fail_threshold) {
                        		work_steal_signal = 0;
					cout << worker->selfIndex << " stopping worksteal" << endl;
					break;
                		}
				usleep(worker->poll_interval);
				success = worker->steal_task();				
			}
			failed_attempts = 0;
			//cout << "Received " << success << " tasks" << endl;
		}
	}
}

int Worker::check_if_task_is_ready(string key) {
	int index = myhash(key.c_str(), svrclient.memberList.size());
	if(index != selfIndex) {
		Package check_package;
		check_package.set_virtualpath(key);
		check_package.set_operation(23);
		string check_str = check_package.SerializeAsString();
		pthread_mutex_lock(&msg_lock);
		int ret = svrclient.svrtosvr(check_str, check_str.size(), index);
		pthread_mutex_unlock(&msg_lock);
		return ret;
	}
	else {
		string value = zht_lookup(key);
		Package check_package;
		check_package.ParseFromString(value);
		return check_package.numwait();
	}
}

int Worker::move_task_to_ready_queue(TaskQueue_Item **qi) 
{
	//pthread_mutex_lock(&w_lock);
	cout << "Index = " << selfIndex << " TRYING TO ACQUIRE READY QUEUE LOCK "  << __LINE__ << endl;
	pthread_mutex_lock(&lock);
	cout << "Index = " << selfIndex << " ACQUIRED READY QUEUE LOCK "  << __LINE__ << endl;
	rqueue.push_back(*qi);
	//wqueue.erase(*qi);
	cout << "Index = " << selfIndex << " TRYING TO RELEASE READY QUEUE LOCK " << __LINE__ << endl;
	pthread_mutex_unlock(&lock);
	cout << "Index = " << selfIndex << " RELEASED READY QUEUE LOCK " << __LINE__ << endl;
	//pthread_mutex_unlock(&w_lock);
}

bool check(TaskQueue_Item *qi) {
	return qi==NULL;
}


void* check_wait_queue(void* args) {
	Worker *worker = (Worker*)args;
	TaskQueue_Item *qi;
        while(ON) {
                while(wqueue.size() > 0) {
			//for(TaskQueue::iterator it = wqueue.begin(); it != wqueue.end(); ++it) {
			int size = wqueue.size();
			for(int i = 0; i < size; i++) {
				//qi = *it;
				qi = wqueue[i];
				if(qi != NULL) {
					int status = worker->check_if_task_is_ready(qi->task_id); //cout << "task = " << qi->task_id << " status = " << status << endl;
					if(status == 0) {
						//cout << "task = " << qi->task_id << " status = " << status << endl;
						int ret = worker->move_task_to_ready_queue(&qi);
						pthread_mutex_lock(&w_lock);
						wqueue[i] = NULL;
						pthread_mutex_unlock(&w_lock);
					}
					/*if(status < 0) {
						cout << "negative numwait" << endl;
					}*/
				}
			}
			pthread_mutex_lock(&w_lock);
			TaskQueue::iterator last = remove_if(wqueue.begin(), wqueue.end(), check);
			wqueue.erase(last, wqueue.end());
			pthread_mutex_unlock(&w_lock);
			sleep(1);
		}
	}
}


//----------------------------------------------------------- HPC Code ----------------------------------------------------------------------

int Worker::get_complete_queue_info()
{
	return cqueue.size();
}

//Executing HPC tasks
//Implements a barrier, so that all the threads start at the same time and ends at the same time.
void* Execute_HPC(void *num_of_ms)
{
	long duration=(long) num_of_ms;
	//cout << "The duration of the task to be run is " << duration << endl;
	//int result=pthread_barrier_wait(&hpc_barrier);
	//cout << "BARRIER RESULT = " << result << endl;
	/*if(result!=0 && result != PTHREAD_BARRIER_SERIAL_THREAD)
	{
		cout <<"Could not wait on barrier" << endl;
		exit(0);
	}*/
	uint32_t exit_code = usleep(duration);
}

//Returns the number of idle cores with this node when asked by the zht server
int32_t Worker::get_idle_core_info(){
	int num_idle_cores = worker->num_idle_cores;
	return num_idle_cores;
}

void Worker::choose_neighbor_for_hpc(int num_hpc_neigh)
{
		hpc_neigh_index=new int[num_hpc_neigh];
		//time_t t;
		//srand((unsigned)time(&t));
		srand(time(NULL));
		int i, ran;
		int x;
		char *flag = new char[num_nodes];
		//cout << "num nodes = " << num_nodes << " num neigh = " << num_neigh << endl;
		/*for(i = 0; i < num_nodes; i++)
		{
			flag[i] = '0';
		}*/
		memset(flag, '0', num_nodes);
		//cout << "flag array set" << endl;
		for(i = 0; i < num_hpc_neigh; i++)
		{
			//cout << " i = " << i << endl;
			ran = rand() % num_nodes;		
			x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
			//cout << "ran a " << ran << " " << selfIndex << " " << x << endl;
			//while(flag[ran] == '1' || !strcmp(all_ips[ran], ip))
			while(flag[ran] == '1' || !x)
			{
				ran = rand() % num_nodes;
				x = hostComp(svrclient.memberList.at(ran), svrclient.memberList.at(selfIndex));
				//cout << "ran = " << ran << " x = " << x << endl;
			}
			flag[ran] = '1';
			//cout << "i = " << i << " ran = " << ran << endl;
			hpc_neigh_index[i] = ran;
			cout << "The selected neighbor indexes is " << hpc_neigh_index[i] << endl;
			
		}
		//cout << "neigh index set" << endl;
		delete flag;
}

//After getting available core information from the nodes, sum it up to check for sufficient number of cores
int Worker::checkForSufficientCores(struct coreInfo idle_cores_info_for_all_nodes[],int num_of_cores,struct coreInfo selected_idle_cores[],int &num_selected_neigh, int &num_hpc_neigh)
{
	int i,sum=0,j=0,result=0;
	cout << "The number of cores required is " << num_of_cores << endl;
	for(i=0;i<num_hpc_neigh;i++)
	{
		num_selected_neigh++;
		sum= sum + idle_cores_info_for_all_nodes[i].cores_available;
		cout << "The sum is " << sum << endl;
		selected_idle_cores[i].cores_available = idle_cores_info_for_all_nodes[i].cores_available;
		selected_idle_cores[i].index=idle_cores_info_for_all_nodes[i].index;
		if(sum>=num_of_cores)
		{
			result = 1;
			exit;
		}
	}
	cout <<"The result returned is " << result << endl;
	return result;
}

//Gets the idle core information in a structure with the format neigh_index:num_of_idle_cores
//returns the structure to the steal_resources method.
void Worker::get_idle_cores_for_neighbors(struct coreInfo idle_cores_info_for_all_nodes[],int num_hpc_neigh)
{
	int i;
	cout << "The number of neighbors is " << num_hpc_neigh << endl;
	for(i=0;i<num_hpc_neigh;i++)
	{
		cout << "INDEX = " << selfIndex << " TRYING TO ACQUIRE THE SVRCLIENT LOCK " << __LINE__ << endl;
		pthread_mutex_lock(&msg_lock);
		cout << "Index = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ << endl;
		int avail_cores = svrclient.svrtosvr(idlestr, idlestr.length(), hpc_neigh_index[i]);
		cout << "INDEX = " << selfIndex << " TRYING TO RELEASE THE SVRCLIENT LOCK " << __LINE__ << endl;
		pthread_mutex_unlock(&msg_lock);
		cout << "INDEX = " << selfIndex << " RELEASED THE SVRCLIENT LOCK " << __LINE__ << endl;
		cout << "The neighbor " << i << "is " << hpc_neigh_index[i];
		idle_cores_info_for_all_nodes[i].index=hpc_neigh_index[i];
		idle_cores_info_for_all_nodes[i].cores_available=avail_cores;
		
		cout << "Neighbor Index = " << idle_cores_info_for_all_nodes[i].index <<"\tNeighbor cores = " << idle_cores_info_for_all_nodes[i].cores_available << endl;
	}
}

//To check if the task is completed in the map
void Worker::InsertMap(Package executed_package)
{
	int num_vector_count,per_vector_count;
	//Retriev the task id to search in the map
	string completed_task_id = executed_package.virtualpath();
	cout << "The completed Task ID is " << completed_task_id << endl;
	vector< vector<string> > tokenize_string = tokenize(executed_package.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
	string executed_cores=tokenize_string.at(0).at(4);
	cout << "INDEX = " << selfIndex << "TRYING TO ACQUIRE THE MAP LOCK " << __LINE__ << endl;
	pthread_mutex_lock(&completed_hpc_map);
	cout << "INDEX = " << selfIndex << "ACQUIRED THE MAP LOCK " << __LINE__ << endl;
	cout << "Old Value = " << completed_hpc_job_map[completed_task_id] << endl;
	completed_hpc_job_map[completed_task_id] = completed_hpc_job_map[completed_task_id] + atoi(executed_cores.c_str());
	cout << "New Value = " << completed_hpc_job_map[completed_task_id] << endl;
	cout << "INDEX = " << selfIndex << "TRYING TO RELEASE THE MAP LOCK " << __LINE__ << endl;
	pthread_mutex_unlock(&completed_hpc_map);
	cout << "INDEX = " << selfIndex << "RELEASED THE MAP LOCK " << __LINE__ << endl;
}

//Resource steal method
//Chooses neighbor to steal resources. Gets the idle core information from each neighbor.
//Checks if the number of idle cores available is sufficient.
int Worker::steal_resources(int num_of_cores,Package recv_pkg,int num_hpc_neigh)
{
	int i,success,index;
	int num_vector_count, per_vector_count,num_selected_neigh=0;
	//Get the task id from the virtual path to append the index to task id for migrating hpc job
	//string &task_id_pack = recv_pkg.virtualpath();
	
	vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
	string task_desc(tokenize_string.at(0).at(1));
	string task_id(tokenize_string.at(0).at(2));
	string sub_time_ns(tokenize_string.at(0).at(3));
	
	struct coreInfo select_core_info[num_hpc_neigh];
	struct coreInfo idle_cores_info_for_all_nodes[num_hpc_neigh];
	choose_neighbor_for_hpc(num_hpc_neigh);
	
	get_idle_cores_for_neighbors(idle_cores_info_for_all_nodes,num_hpc_neigh);
	
	//cout << "The idle core information for all nodes received is "<< endl;
	for(i=0;i<num_hpc_neigh;i++)
	{
		cout << endl ;
		cout << "SELF INDEX = " << selfIndex << "Index = " << idle_cores_info_for_all_nodes[i].index << "\tCores Available: " << idle_cores_info_for_all_nodes[i].cores_available << endl;
	}


	success = worker->checkForSufficientCores(idle_cores_info_for_all_nodes,num_of_cores,select_core_info,num_selected_neigh,num_hpc_neigh);
	
	if(success == 0)
	{
		//Back off time to try again
		
		//Release all the resources - That is unlock the ready queues locked while retrieving idle core information
		cout << "Index = " << selfIndex << " Release all resources since sufficient amount in not acquired " << endl;
		for(i=0;i<num_hpc_neigh;i++)
		{
			cout << " INDEX = " << selfIndex << " TRYING TO ACQUIRE SVRCLIENT LOCK " << __LINE__ << endl;
			pthread_mutex_lock(&msg_lock);
			cout << " INDEX = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ << endl;
			int avail_cores = svrclient.svrtosvr(releasestr, releasestr.length(), idle_cores_info_for_all_nodes[i].index);
			cout << " INDEX = " << selfIndex << " TRYING TO RELEASE SVRCLIENT LOCK " << __LINE__ << endl;
			
			pthread_mutex_unlock(&msg_lock);
			cout << " INDEX = " << selfIndex << " RELEASED SVRCLIENT LOCK " << __LINE__ << endl;
		}
		cout <<"Released all resources and returning the success value which is " << success << endl;
		return success;	
	}
	else
	{
		//Release those resources that are not required
		cout << "Index = " << selfIndex << " Release those resources which is not required " << endl;
		for(i=num_selected_neigh;i<num_hpc_neigh;i++)
                {
			cout << " INDEX = " << selfIndex << " TRYING TO ACQUIRE SVRCLIENT LOCK " << __LINE__ <<endl;
                        pthread_mutex_lock(&msg_lock);
			cout << " INDEX = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ << endl;
                        int avail_cores = svrclient.svrtosvr(releasestr, releasestr.length(), idle_cores_info_for_all_nodes[i].index);
			cout << " INDEX = " << selfIndex << " TRYING TO RELEASE SVRCLIENT LOCK " << __LINE__ << endl;
                        pthread_mutex_unlock(&msg_lock);
			cout << " INDEX = " << selfIndex << " RELEASED SVRCLIENT LOCK " << __LINE__ << endl;
                }
		cout <<"Following is the selected core information" << endl;
		for(i=0;i<num_selected_neigh;i++)
		{
			cout << "selected Index - " << select_core_info[i].index << "\tAvailable Cores - " << select_core_info[i].cores_available << endl;  
		}
		//Migrate tasks to the nodes mentioned
		int resource_division = num_of_cores;
		for(i=0;i<num_selected_neigh;i++)
		{
			Package migratePackage;
			stringstream task_id_ss;
			task_id_ss << task_id; 
			task_id_ss << "_"; 
			task_id_ss << select_core_info[i].index;
			migratePackage.set_virtualpath(task_id_ss.str());
			//migratePackage.set_operation(3); //Insert task and its description to NOVHT - Kiran
			
			cout << "The task to be migrated is " << task_id_ss.str() << endl;
			//Build real path for the tasks
			stringstream package_content_ss;
			migratePackage.set_nummoves(0);
			package_content_ss << "NULL"; package_content_ss << "\'"; 				// Task completion status
			package_content_ss << task_desc; package_content_ss << "\'"; 				// Task Description
			package_content_ss << task_id;	package_content_ss << "\'"; 			// Task ID
			package_content_ss << sub_time_ns; package_content_ss << "\'";  // Task Submission Time
			if(resource_division >= select_core_info[i].cores_available)
			{
				cout << "Resource division = " << resource_division << endl;
				package_content_ss << select_core_info[i].cores_available;
				package_content_ss << "\'";
				package_content_ss << selfIndex;
				package_content_ss << "\'";
				package_content_ss << "\"";
				resource_division = abs(resource_division - select_core_info[i].cores_available);
				cout << "New Resource division = " << resource_division << endl;
			}
			else
			{
				cout << "Resource division at else is " << resource_division << endl;
				package_content_ss << resource_division;
				package_content_ss << "\'";
				package_content_ss << selfIndex;
                                package_content_ss << "\'";
				package_content_ss << "\"";
			}
			package_content_ss << ""; // List of tasks to be notified after finishing execution
			migratePackage.set_realfullpath(package_content_ss.str());
	
			cout << "The package content is" << package_content_ss.str() << endl;
			
			//Insert the metadata into ZHT
			//migratePackage.set_operation(20);
			
			//Insert the metadata into ZHT
			cout << "Calling zht_insert" << endl;
			int valrec = worker->zht_insert(migratePackage.SerializeAsString());
			cout << "The value received is " << valrec << endl;

			/*cout<<"Retrieve the task inserted into ZHT" << endl;
			string retriev = worker->zht_lookup(task_id_ss.str());
			cout << "The task That was updated to is " << retriev << endl;*/
			
			
			//Set operation for migrating HPC task
			//Call ZHT server-to-server method to transfer the task
			cout << "BEFORE SETTING OPERATION FOR MIGRATION" <<endl;
			migratePackage.set_operation(17);
			cout << "AFTER SETTING OPERATION FOR MIGRATION" <<endl;
			string migrate_str = migratePackage.SerializeAsString();
			cout << "MIGRATE STRING IS  " << migrate_str << endl;
			cout << "AFTER THE MIGRATE STRING, TASK HAS TO BE SENT TO ";
			cout << "INDEX = " << selfIndex << " TRYING TO ACQUIRE SVRCLIENT LOCK " << __LINE__ << endl;
			pthread_mutex_lock(&msg_lock);
			cout << "INDEX = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ <<endl;
			cout << "The index to which the task is being sent is " << select_core_info[i].index;
			int ret = svrclient.svrtosvr(migrate_str, migrate_str.size(), select_core_info[i].index);
			cout << "The return value from the server to migrate is " << ret << endl;
			cout << "INDEX = " <<  selfIndex << " TRYING TO RELEASE SVRCLIENT LOCK " << __LINE__ << endl;
			pthread_mutex_unlock(&msg_lock);
			cout << "INDEX = " << selfIndex << " RELEASED SVRCLIENT LOCK " << __LINE__ << endl;
			
		}
		cout <<"Tasks successfully migrated and the return value is " << success << endl;
		return success;
	}
}


int Worker::insert_task_to_ready_queue(TaskQueue_Item **qi) 
{
	cout << "INDEX = " << selfIndex << " TRYING TO ACQUIRE THE READY QUEUE LOCK " << __LINE__ << endl;
	struct timespec *ts = (struct timespec *)(malloc(sizeof(struct timespec)));
        //gettimeofday(ts);
        ts->tv_sec=1;
        ts->tv_nsec=0;
        //int ret = pthread_mutex_timedlock(&lock,ts);
	//cout << "Ready queue return value is " << ret << endl;
	/*while(ret!=0)
	{
		cout << "Ready queue lock not available " << endl;
		usleep(100000);
		ret = pthread_mutex_timedlock(&lock,ts);
	}*/
        //pthread_mutex_lock(&lock);
	cout << "INDEX = " << selfIndex << " ACQUIRED TO RELEASE THE READY QUEUE LOCK " << __LINE__ << endl;
	rqueue.push_front(*qi);
	
	cout << "Unlock the ready queue as the task is inserted" << endl;
	cout << "INDEX = " << selfIndex << " TRYING TO RELEASE THE READY QUEUE LOCK " << __LINE__ << endl; 
        //pthread_mutex_unlock(&lock);
	cout << "INDEX = " << selfIndex << " RELEASED THE READY QUEUE LOCK " << __LINE__ << endl;

	//Not locked while getting resources.
	//cout << "INDEX = " << selfIndex << " TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
	//pthread_mutex_unlock(&mutex_idle);
	//cout << "INDEX = " << selfIndex << " RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
}

//Insert recieved HPC task into reaady queue for execution
//Remove lock on the ready queue.
void Worker::Receive_HPC_Task(Package migratePackage)
{
	int num_vector_count, per_vector_count;
	cout << "Recieving a new task" << endl;
	//vector< vector<string> > tokenize_string = tokenize(migratePackage.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
	cout << "The task id is " << migratePackage.virtualpath() <<  endl;
	TaskQueue_Item *insert_HPC_Item = new TaskQueue_Item();
//	cout << "Task ID is " << tokenize_string.at(0).at(1) << endl;
	insert_HPC_Item->task_id =  migratePackage.virtualpath();
	cout << "Task id is " << insert_HPC_Item->task_id << endl;
	//rqueue.push_back(*insert_HPC_Item);
	worker->insert_task_to_ready_queue(&insert_HPC_Item);
	cout << "The task is inserted into the ready queue " << endl;
	//Unclock the ready queue locked while getting idle core information
	
	//cout << "The ready queue is unlocked" << endl;
}

void Worker::Release_Resource()
{
	cout << "INDEX = " << selfIndex << "TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
	pthread_mutex_unlock(&mutex_idle);
	cout << "INDEX = " << selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
	cout << "INDEX = " << selfIndex << "TRYING TO RELEASE THE READY QUEUE LOCK " << __LINE__ << endl;
	pthread_mutex_unlock(&lock);
	cout << "INDEX = " << selfIndex << "RELEASED THE READY QUEUE LOCK " << __LINE__ << endl;
}

void Worker::Send_HPC_Results(vector< vector<string> > tokenize_string)
{
	string source_id=tokenize_string.at(0).at(2);
        cout << "The source id is " << source_id << endl;
        Package sourcePackage;
        sourcePackage.set_virtualpath(source_id);
        stringstream source_package_content_ss;
        source_package_content_ss << "NULL";
        source_package_content_ss << "\'";
        source_package_content_ss << tokenize_string.at(0).at(1);
        source_package_content_ss << "\'";
        source_package_content_ss << tokenize_string.at(0).at(2);
        source_package_content_ss << "\'";
        source_package_content_ss << tokenize_string.at(0).at(3);
        source_package_content_ss << "\'";  // Task Submission Time
        source_package_content_ss << tokenize_string.at(0).at(4);
        source_package_content_ss << "\'";
        source_package_content_ss << 0;
        source_package_content_ss <<"\'";
        source_package_content_ss << "\"";
	source_package_content_ss << "";
        // List of tasks to be notified after finishing execution
        sourcePackage.set_realfullpath(source_package_content_ss.str());
        sourcePackage.set_operation(19);
        string source_str = sourcePackage.SerializeAsString();
	cout << " INDEX = " << selfIndex << " TRYING TO ACQUIRE SVRCLIENT LOCK " << __LINE__ << endl;
        pthread_mutex_lock(&msg_lock);
	cout << " INDEX = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ << endl;
        int ret = svrclient.svrtosvr(source_str, source_str.size(),atoi(tokenize_string.at(0).at(5).c_str()));
	cout << " INDEX = " << selfIndex << " TRYING TO RELEASE SVRCLIENT LOCK " << __LINE__ << endl;
	pthread_mutex_unlock(&msg_lock);
	cout << " INDEX = " << selfIndex << " RELEASED SVRCLIENT LOCK " << __LINE__ <<endl;
	cout << "The result is sent to the source with id " << atoi(tokenize_string.at(0).at(5).c_str()) << endl;

}

/*double getTime_usec() 
{
       struct timeval tp;
       gettimeofday(&tp, NULL);
       return static_cast<double>(tp.tv_sec) * 1E6
                       + static_cast<double>(tp.tv_usec);

}*/

void* check_ready_queue_for_migrated_tasks(void* args)
{
	Worker *worker = (Worker*)args;
        int i,success, hpc_threshold, exec_success;
        TaskQueue_Item *qi;
        while(ON)
        {
                while(rqueue.size() > 0)
                {
                        hpc_threshold=0;
                        exec_success=0;
                        cout << "The size of the ready queue is " << rqueue.size() << endl;
                        pthread_mutex_lock(&lock);
                        if(rqueue.size() > 0)
                        { 
                                qi = rqueue.front();
                                //rqueue.pop_front();
                                if(qi->task_id == "1000")
                                {
                                        std::vector<string> dummy;
                                        cqueue.push_back(make_pair(qi->task_id,dummy));
                                        continue;
                                }
                        }
                        else
                        {
                                pthread_mutex_unlock(&lock);
                                continue;
                        }
                        //Retrieving the package 
                        cout << "The task id is " << qi->task_id << endl;
                        string value = worker->zht_lookup(qi->task_id);
                        cout << "The package value is " << value << endl;
                        Package recv_pkg;
                        recv_pkg.ParseFromString(value); //cout << "check_ready_queue: task " << qi->task_id << " node history = " << recv_pkg.nodehistory() << endl;
                        int num_vector_count, per_vector_count;
                        cout << "Tokenize string nested vector " << endl;
                        vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
			stringstream task_index_ss;
			task_index_ss << tokenize_string.at(0).at(5);
                        cout << "The task index is " << task_index_ss;
                        string task_index = task_index_ss.str();

			string source_id=tokenize_string.at(0).at(2);
			int task_source=atoi(source_id.c_str());
			cout << "The task source id in the hpc execution thread is " << task_source << endl;
			if(atoi(task_index.c_str()) != -2 && atoi(task_index.c_str()) != -1)
			{
			//if(task_source != worker->selfIndex)
			//{
				rqueue.pop_front();
				pthread_mutex_unlock(&lock);
				stringstream num_of_cores_ss;
				num_of_cores_ss << tokenize_string.at(0).at(4);
				string num_of_cores = num_of_cores_ss.str();
				pthread_t hpc_threads[atoi(num_of_cores.c_str())];
				stringstream duration_ss;
                        	try
                        	{
                                	duration_ss <<  tokenize_string.at(0).at(1);
                        	}
                        	catch (exception& e)
                        	{
                                	cout << "void* check_ready_queue_for_migrated_tasks: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
                                	cout << "void* check_ready_queue_for_migrated_tasks: (tokenize_string.at(0).at(1)) " << " " << e.what() << endl;
                                	cout << "void* checzk_ready_queue: value = " << value << endl;
                               	 	exit(1);
                        	}
                        	long duration;
                        	duration_ss >> duration;
				for(i=0;i<atoi(num_of_cores.c_str());i++)
                                {
                                 	cout << "Creating thread " << i << "for hpc task" << endl;
                                        pthread_create(&hpc_threads[i],NULL,Execute_HPC,(void*)duration);
				}
				for(i=0;i<atoi(num_of_cores.c_str());i++)
                                {
                                	pthread_join(hpc_threads[i],NULL);
                                }
				worker->Send_HPC_Results(tokenize_string);
			}
			else
			{	
				pthread_mutex_unlock(&lock);
				usleep(10000);
			}
		}
	}
}

//-------------------------------------------- End of HPC code ---------------------------------------------------------


static int work_exec_flag = 0;
// thread to monitor ready queue and execute tasks based on num of cores availability
void* check_ready_queue(void* args) 
{
	Worker *worker = (Worker*)args;
	int i,success, hpc_threshold, exec_success;
	TaskQueue_Item *qi;
	while(ON)
	{
		while(rqueue.size() > 0)
		{
			hpc_threshold=0;
			exec_success=0;
			cout << "The size of the ready queue is " << rqueue.size() << endl;
			pthread_mutex_lock(&lock);
			if(rqueue.size() > 0) 
			{						
				qi = rqueue.front();
				rqueue.pop_front();
				if(qi->task_id == "1000")
				{
					std::vector<string> dummy;
					cqueue.push_back(make_pair(qi->task_id,dummy));
					continue;
				}
			}
			else
			{
				pthread_mutex_unlock(&lock);
				continue;
			}
			//Retrieving the package 
			cout << "The task id is " << qi->task_id << endl;
			string value = worker->zht_lookup(qi->task_id);
			cout << "The package value is " << value << endl;
			Package recv_pkg;
			recv_pkg.ParseFromString(value); //cout << "check_ready_queue: task " << qi->task_id << " node history = " << recv_pkg.nodehistory() << endl;
			int num_vector_count, per_vector_count;
			cout << "Tokenize string nested vector " << endl;
			vector< vector<string> > tokenize_string = tokenize(recv_pkg.realfullpath(), '\"', '\'', num_vector_count, per_vector_count);
			/*cout << "Trying to remove the invalid task id's " << endl;
			if(qi->task_id == "1")
                        {
				cout << "Push invalid task id into complete queue " << endl;
                        	cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(0)));
				cout << "Done inserting the invalid task id into queue " << endl;
                                continue;
                        }*/
			cout << "Completed tokenization " << endl;
			stringstream num_of_cores_ss;
			cout << "Retrieving number of cores " << endl;
			num_of_cores_ss << tokenize_string.at(0).at(4);
			cout << "The number of cores needed is " << num_of_cores_ss.str() << endl;
			string num_of_cores;
			num_of_cores = num_of_cores_ss.str();

			stringstream task_index_ss;
			cout << "THE TOKENIZE STRING IS " << recv_pkg.realfullpath() << endl;
			task_index_ss << tokenize_string.at(0).at(5);
			cout << "The task index is " << task_index_ss;
			string task_index = task_index_ss.str();

			stringstream duration_ss;
			try 
			{
				duration_ss <<  tokenize_string.at(0).at(1);
			}
			catch (exception& e) 
			{
				cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
				cout << "void* check_ready_queue: (tokenize_string.at(0).at(1)) " << " " << e.what() << endl;
				cout << "void* checzk_ready_queue: value = " << value << endl;
				exit(1);
			}
			long duration;
			duration_ss >> duration;

			/*if(!work_exec_flag)
                        {
                        	work_exec_flag = 1;
                                worker_start << worker->ip << ":" << worker->selfIndex << " Got jobs..Started excuting" << endl;
                        }*/
		
			//If MTC tasks, that is it needs only 1 core
			if(atoi(num_of_cores.c_str())==1&&atoi(task_index.c_str())==-2)
			{
				pthread_mutex_unlock(&lock);
				//Decrement the number of cores while it is in use. 
				pthread_mutex_lock(&mutex_idle);
				worker->num_idle_cores--;
				pthread_mutex_unlock(&mutex_idle);
				if(!work_exec_flag)
				{
					work_exec_flag = 1;
					worker_start << worker->ip << ":" << worker->selfIndex << " Got jobs..Started excuting" << endl;
				}
				string client_id;
				try 
				{
					client_id = tokenize_string.at(0).at(2);
				}
				catch (exception& e) 
				{
					cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
					cout << "void* check_ready_queue: (tokenize_string.at(0).at(2)) " << " " << e.what() << endl;
					exit(1);
				}
				uint64_t sub_time;
				try 
				{
					stringstream sub_time_ss;
					sub_time_ss << tokenize_string.at(0).at(3);
					sub_time_ss >> sub_time;
				}
				catch (exception& e)
				{
					cout << "void* check_ready_queue: num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
					cout << "void* check_ready_queue: (tokenize_string.at(0).at(3)) " << " " << e.what() << endl;
					exit(1);
				}
				timespec task_start_time, task_end_time;
				clock_gettime(CLOCK_REALTIME, &task_start_time);
				//uint32_t exit_code = sleep(duration);
				uint32_t exit_code = usleep(duration);
				clock_gettime(CLOCK_REALTIME, &task_end_time);
				// push completed task into complete queue
				pthread_mutex_lock(&c_lock);
				cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(1)));
				pthread_mutex_unlock(&c_lock);
					// append completed task
				uint64_t st = (uint64_t)task_start_time.tv_sec * 1000000000 + (uint64_t)task_start_time.tv_nsec;
				uint64_t et = (uint64_t)task_end_time.tv_sec * 1000000000 + (uint64_t)task_end_time.tv_nsec;
				timespec diff = timediff(task_start_time, task_end_time);
				cout << "INDEX = " << worker->selfIndex << "TRYING TO ACQUIRE THE MUTEX IDLE LOCK " << __LINE__ << endl;

				pthread_mutex_lock(&mutex_idle);
				cout << "INDEX = " << worker->selfIndex << "ACQUIRED THE MUTEX IDLE LOCK " << __LINE__ << endl;
				worker->num_idle_cores++; task_comp_count++;
				cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
				pthread_mutex_unlock(&mutex_idle);
				cout << "INDEX = " << worker->selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
				if(LOGGING)
				{
					string fin_str;
					stringstream out;
					out << qi->task_id << "+" << client_id << " exitcode " << " node history = " << recv_pkg.nodehistory() << exit_code << " Interval " << diff.tv_sec << " S  " << diff.tv_nsec << " NS" << " server " << worker->ip;
					fin_str = out.str();
					pthread_mutex_lock(&mutex_finish);
					fin_fp << fin_str << endl;
					pthread_mutex_unlock(&mutex_finish);
				}						
				delete qi;
			}
			else if(atoi(num_of_cores.c_str())>=1)
			{
				
				pthread_mutex_lock(&mutex_idle);

				int num_cores_avail = worker->num_idle_cores;
				cout << "The number of idle cores available is " << num_cores_avail << endl;
				int num_cores_in_system=2;//sysconf(_SC_NPROCESSORS_ONLN);
				cout << "The number of cores in the system is " << num_cores_in_system << endl;
				if(num_cores_in_system >= atoi(num_of_cores.c_str()))
				{
					cout << "Since number of cores in the system is more than needed for this hpc job, it can be run on a single system" << endl;
					if(atoi(num_of_cores.c_str())>worker->num_idle_cores)
					{
						pthread_mutex_unlock(&mutex_idle);
						while(atoi(num_of_cores.c_str())>worker->num_idle_cores)
                                        	{
							sleep(1);
                                        	}	
						pthread_mutex_lock(&mutex_idle);
					}
					
					pthread_t hpc_threads[atoi(num_of_cores.c_str())];
					//while(exec_success==0)
					cout << "The hpc job now has the required number of free cores." << endl;
					//Decrement the number of idle cores. Unlock the number of idle cores for other threads to probe it.
					worker->num_idle_cores=worker->num_idle_cores-atoi(num_of_cores.c_str());
					cout << "The NEW number of idle cores after using it for HPC is " << worker->num_idle_cores << endl;
					cout << "Unlock mutex idle now" << endl;
					cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
					pthread_mutex_unlock(&mutex_idle);
					cout << "INDEX = " << worker->selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
					//Unlock the ready queue as we have got the cores needed.
					cout << " Unlock ready queue now " << endl;
					cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE READY LOCK " << __LINE__ << endl;
					pthread_mutex_unlock(&lock);
					cout << "INDEX = " << worker->selfIndex << "RELEASED THE READY QUEUE LOCK " << __LINE__ << endl;
					//pthread_t hpc_threads[atoi(num_of_cores.c_str())];
					//Barrier initialization
					/*if(pthread_barrier_init(&hpc_barrier,NULL,atoi(num_of_cores.c_str())))
					{
						cout << "Could not create a barrier" << endl;
						exit(0);
					}*/
					int i;
					//Create threads to run hpc on the available cores.
					//This calls the Execute_HPC method
					for(i=0;i<atoi(num_of_cores.c_str());i++)
					{
						//Unlock the ready queu			
						cout << "Creating thread " << i << "for hpc task" << endl; 
						pthread_create(&hpc_threads[i],NULL,Execute_HPC,(void*)duration);
					}
					//After creating threads, lock on number of idle cores can be removed
					//pthread_mutex_unlock(&mutex_idle);
					//Wait for all the threads to join after the execution
					for(i=0;i<atoi(num_of_cores.c_str());i++)
					{
						pthread_join(hpc_threads[i],NULL);
					}
					cout << "All the threads executed the task" << endl;
					exec_success=1;
					cout << "INDEX = " << worker->selfIndex << "TRYING TO ACQUIRE THE MUTEX IDLE LOCK" << __LINE__ << endl;
					pthread_mutex_lock(&mutex_idle);
					cout << "INDEX = " << worker->selfIndex << "ACQUIRED THE MUTEX IDLE LOCK" << __LINE__ << endl;
					worker->num_idle_cores=worker->num_idle_cores+atoi(num_of_cores.c_str());
					cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
					pthread_mutex_unlock(&mutex_idle);
					cout << "INDEX = " << worker->selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
					if(atoi(task_index.c_str()) == -1)
					{
						//Place the task in the complete queue
						cout << "INDEX = " << worker->selfIndex << "TRYING TO ACQUIRE THE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						pthread_mutex_lock(&c_lock);
						cout << "INDEX = " << worker->selfIndex << "ACQUIRED THE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(1)));
						cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						cout << "THE SIZE OF COMPLETE QUEUE IS " << cqueue.size() << endl;
						pthread_mutex_unlock(&c_lock);
						cout << "INDEX = " << worker->selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
						double currentmillis = getTime_usec();
	                                        cout << "KIRAN - TASK COMPLETED AT " << qi->task_id << " - " << currentmillis << endl;
					}
					else
					{
						//Build the client ID and Task ID excluding index sent by the source
						worker->Send_HPC_Results(tokenize_string);
					}
					//}
					//else
					//{	
					//Need to loop until idle cores are available. 
					//	cout << "Wait until enough cores are available " << endl;
					//		exec_success=0;
					//	}
				}
			
				else
				{

					if(!work_exec_flag)
                                	{
                                        	work_exec_flag = 1;
	                                        worker_start << worker->ip << ":" << worker->selfIndex << " Got jobs..Started excuting" << endl;
                                	}
					cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE MUTEX IDLE LOCK " << __LINE__ << endl;
					pthread_mutex_unlock(&mutex_idle);
					cout << "INDEX = " << worker->selfIndex << "RELEASED THE MUTEX IDLE LOCK " << __LINE__ << endl;
					cout << "INDEX = " << worker->selfIndex << "TRYING TO RELEASE THE READY QUEUE LOCK " << __LINE__ << endl;
					pthread_mutex_unlock(&lock);
					cout << "INDEX = " << worker->selfIndex << "RELEASED THE READY QUEUE LOCK " << __LINE__ << endl;
				
					//Start resource stealing
					cout << "Start resource stealing now" << endl;
					float f_num_of_cores = (float) atoi(num_of_cores.c_str());
					float f_num_cores_in_system = (float) num_cores_in_system;
		
					int num_hpc_neigh = (int) ceil(f_num_of_cores/f_num_cores_in_system);

					
					cout << "The number of hpc neighbors to be selected is " << num_hpc_neigh << endl;

					success = worker->steal_resources(atoi(num_of_cores.c_str()),recv_pkg,num_hpc_neigh);
					cout << "The result received at the resource stealing initialization is " << success << endl;
					while(success==0)
					{
						cout << "HPC threshold value is " << hpc_threshold << endl;
						if(hpc_threshold <= 2)
						{
							cout << "HPC THRESHOLD IS " << hpc_threshold << endl;
							//sleep(worker->selfIndex * 1);
							sleep(1);
							cout << "STARTING RESOURCE STEALING AGAIN AFTER BACK OFF " <<  endl;
							success = worker->steal_resources(atoi(num_of_cores.c_str()),recv_pkg,num_hpc_neigh);
							cout << "BACKOFF implemented, New success value is " << success << endl;
							hpc_threshold++;
						}
						else
						{
							//put back the task on the ready queue
							cout << "HPC threshold value is " << hpc_threshold << endl;
							TaskQueue_Item *insert_HPC_Item;
							insert_HPC_Item = new TaskQueue_Item();
							cout << "Task ID is " << tokenize_string.at(0).at(1) << endl;
							insert_HPC_Item->task_id=tokenize_string.at(0).at(1);
							worker->move_task_to_ready_queue(&insert_HPC_Item);
							break;
						}
					}
					hpc_threshold=0;
					if(success==1)
					{
						//Wait until the hpc task is completed on all the cores. 
						while(completed_hpc_job_map[qi->task_id] != atoi(num_of_cores.c_str()));
						//Completed the hppc task, push it to complete queue
						cout << "HPC task completed and will be placed in complete queue"<< endl;
						cout << "INDEX = " << worker->selfIndex << "TRYING TO ACQUIRE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						pthread_mutex_lock(&c_lock);
						cout << "INDEX = " << worker->selfIndex << "ACUIRED THE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						cqueue.push_back(make_pair(qi->task_id, tokenize_string.at(1)));
						cout << qi->task_id << "Task completed" << endl;
						pthread_mutex_unlock(&c_lock);
						double currentmillis = getTime_usec();
                	                        cout << "KIRAN - TASK COMPLETED AT " << qi->task_id << " - " << currentmillis << endl;
        					//int milli = startTime.tv_usec / 1000;
						cout << "INDEX = " << worker->selfIndex << "RELEASED THE COMPLETE QUEUE LOCK " << __LINE__ << endl;
						cout << "The size of complete queue is " << cqueue.size() << endl;
					}
				}
			}

		}
		
	}	
}
int Worker::notify(ComPair &compair) {
	map<uint32_t, NodeList> update_map = worker->get_map(compair.second);
	int update_ret = worker->zht_update(update_map, "numwait", selfIndex);
	nots += compair.second.size(); log_fp << "nots = " << nots << endl;
	return update_ret;
	/*cout << "task = " << compair.first << " notify list: ";
	for(int l = 0; l < compair.second.size(); l++) {
		cout << compair.second.at(l) << " ";
	} cout << endl;*/
}

void* check_complete_queue(void* args) {
	Worker *worker = (Worker*)args;

        ComPair compair;
        while(ON) {
                while(cqueue.size() > 0) {
                                pthread_mutex_lock(&c_lock);
                                        if(cqueue.size() > 0) {
                                                compair = cqueue.front();
                                                //cqueue.pop_front();
                                        }
                                        else {
                                                pthread_mutex_unlock(&c_lock);
                                                continue;
                                        }
                                pthread_mutex_unlock(&c_lock);
				worker->notify(compair);
		}
	}
}

int32_t Worker::get_load_info() {
	return (rqueue.size() - num_idle_cores);
}



int32_t Worker::get_monitoring_info() {
	if (LOGGING) {
		log_fp << "rqueue = " << rqueue.size() << " mqueue = " << mqueue.size() << " wqueue = " << wqueue.size() << endl;
	}
	return (((rqueue.size() + mqueue.size() + wqueue.size()) * 10) + num_idle_cores);
}

int32_t Worker::get_numtasks_to_steal() {
	return ((rqueue.size() - num_idle_cores)/2);
}

string Worker::zht_lookup(string key) {
	string task_str;

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(1);
	string lookup_str = package.SerializeAsString();
	int index;
	svrclient.str2Host(lookup_str, index);*/

	cout << "Trying to get the index for ZHT lookup" << endl;

	int index = myhash(key.c_str(), svrclient.memberList.size());

	cout << "Received the index at the ZHT server - " << index << endl;

	if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(1);
	        string lookup_str = package.SerializeAsString();

		cout << "INDEX = " << selfIndex << " TRYING TO ACQUIRE SVRCLIENT LOCK " << __LINE__ << endl;
		pthread_mutex_lock(&msg_lock);
		cout << "INDEX = " << selfIndex << " ACQUIRED SVRCLIENT LOCK " << __LINE__ << endl;
		int ret = svrclient.lookup(lookup_str, task_str);
		cout << "INDEX = " << selfIndex << " The ZHT lookup value is " << ret << endl;
		cout << "INDEX = " << selfIndex << " TRYING TO RELEASE SVRCLIENT LOCK " << __LINE__ << endl;
		pthread_mutex_unlock(&msg_lock);
		cout << "INDEX = " << selfIndex << " RELEASED SVRCLIENT LOCK " << __LINE__ << endl;

		Package task_pkg;
        	task_pkg.ParseFromString(task_str);
		//cout << "remote lookup: string = " << task_str << " str size = " << task_str.size() << endl;
	        //cout << "remote lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	else {
		string *result = pmap->get(key);
		if (result == NULL) {
			//cout << "lookup find nothing. key = " << key << endl;
			string nullString = "Empty";
			return nullString;
		}
		task_str = *result;
		//Package task_pkg;
        	//task_pkg.ParseFromString(task_str);
	        //cout << "local lookup: task = " << task_pkg.virtualpath() << " nodehistory = " << task_pkg.nodehistory() << endl;
	}
	return task_str;
	/*Package task_pkg;
	task_pkg.ParseFromString(task_str);
	return task_pkg.realfullpath();*/
}

int Worker::zht_insert(string str) {
	Package package;
	package.ParseFromString(str);
        package.set_operation(3);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	cout << "The self index is " << selfIndex << endl;
	if(index != selfIndex) { //cout << "NOOOOO..index = " << index << endl;
		cout << "The index in if is " << index << endl;
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.insert(str); 
                pthread_mutex_unlock(&msg_lock);
		cout << "DONE inserting into ZHT server " << endl; 
		return ret;
        }
	else {
		cout << "The index at else is " << index << endl;
		string key = package.virtualpath(); //cout << "key = " << key << endl;
		//pthread_mutex_lock(&zht_lock);
		cout << "the key is " << key << endl;
		int ret = pmap->put(key, str);
		cout << "The return value is " << ret << endl;
		//pthread_mutex_unlock(&zht_lock);
		if (ret != 0) {
			cerr << "insert error: key = " << key << " ret = " << ret << endl;
			return -3;
		}
		else
			return 0;
	}
}

int Worker::zht_remove(string key) {

	/*Package package;
	package.set_virtualpath(key);
	package.set_operation(2);
	string str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);*/

	int index = myhash(key.c_str(), svrclient.memberList.size());

        if(index != selfIndex) {
		Package package;
	        package.set_virtualpath(key);
        	package.set_operation(2);
	        string str = package.SerializeAsString();
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.remove(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
                int ret = pmap->remove(key);
		if (ret != 0) {
			cerr << "DB Error: fail to remove :ret= " << ret << endl;
			return -2;
		} else
			return 0; //succeed.
        }

}

int Worker::zht_append(string str) {
	Package package;
	package.ParseFromString(str);
	package.set_operation(4);
	str = package.SerializeAsString();

	int index;
        svrclient.str2Host(str, index);

	if(index != selfIndex) {
                pthread_mutex_lock(&msg_lock);
                int ret = svrclient.append(str);
                pthread_mutex_unlock(&msg_lock);
                return ret;
        }
        else {
		string key = package.virtualpath();
		int ret = pmap->append(key, str);
		if (ret != 0) {
			cerr << "Append error: ret = " << ret << endl;
			return -4;
		} else
			return 0;
	}
}

int Worker::update_nodehistory(uint32_t currnode, string alltasks) {
	int num_vector_count, per_vector_count;
	vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	//cout << "Worker = " << selfIndex << " num_vector_count = " << num_vector_count << " per_vector_count = " << per_vector_count << endl;
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                	string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);

			int index = myhash(taskid.c_str(), num_nodes);
                        if(index != selfIndex) {
                                cout << "something wrong..doing remote update_nodehistory index = " << index << " selfIndex = " << selfIndex << endl;
                        }

			// update number of moves (increment)
			uint32_t old_nummoves = recv_pkg.nummoves();
			recv_pkg.set_nummoves(old_nummoves+1);

			// update current location of task
			recv_pkg.set_currnode(currnode);

			// update task migration history
			stringstream nodehistory_ss;
			nodehistory_ss << currnode << "\'";
			string new_nodehistory(nodehistory_ss.str());
			new_nodehistory.append(recv_pkg.nodehistory()); //cout << "update_nodehistory: task " << recv_pkg.virtualpath() << " node history = " << recv_pkg.nodehistory();
			recv_pkg.set_nodehistory(new_nodehistory); //cout << " node history = " << recv_pkg.nodehistory() << endl;

			// insert updated task into ZHT
			int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_nodehistory: zht_insert error ret = " << ret << endl;
				exit(1);
			}
                }
                catch (exception& e) {
                	cout << "update_nodehistory: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
	}
	return 0;
}

int Worker::update_numwait(string alltasks) {
	int num_vector_count, per_vector_count;
        vector< vector<string> > tokenize_string = tokenize(alltasks, '\"', '\'', num_vector_count, per_vector_count);
	uint32_t num_nodes = svrclient.memberList.size();
	for(int i = 0; i < num_vector_count; i++) {
		for(int j = 0; j < per_vector_count; j++) {
                try {
                        string &taskid = tokenize_string.at(i).at(j);
			string value = zht_lookup(taskid);
			Package recv_pkg;
                        recv_pkg.ParseFromString(value);
	
			int index = myhash(taskid.c_str(), num_nodes);
			if(index != selfIndex) {
				cout << "something wrong..doing remote update_numwait: index = " << index << " selfIndex = " << selfIndex << endl;
			}

			// update number of tasks to wait (decrement)
			uint32_t old_numwait = recv_pkg.numwait();
                        recv_pkg.set_numwait(old_numwait-1);
			notr++;
			if(LOGGING) {
				if(old_numwait-1 == 0) {
					log_fp << "task = " << taskid << " is ready" << endl;
				}
			}

			// insert updated task into ZHT
                        int ret = zht_insert(recv_pkg.SerializeAsString());
			if (ret != 0) {
				cout << "update_numwait: old_numwait = " << old_numwait << endl;
                                cout << "update_numwait: zht_insert error ret = " << ret << " key = " << taskid << " index = " << index << " selfindex = " << selfIndex << endl;
                                exit(1);
                        }
                }
                catch (exception& e) {
                        cout << "update_numwait: (tokenize_string.at(i).at(0)) " << " " << e.what() << endl;
                        exit(1);
                }
		}
        }
	log_fp << "notr = " << notr << endl;
	return 0;
}

int Worker::update(Package &package) {
	string field(package.virtualpath());
	if(field.compare("nodehistory") == 0) {
		return update_nodehistory(package.currnode(), package.realfullpath());
	}
	else if(field.compare("numwait") == 0) {
		return update_numwait(package.realfullpath());
	}
}

int Worker::zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid = 0) {

	Package package;
        package.set_operation(25);
	package.set_virtualpath(field);
        if(!field.compare("nodehistory")) {
                package.set_currnode(toid);
        } //cout << "deque size = ";
	for(map<uint32_t, NodeList>::iterator map_it = update_map.begin(); map_it != update_map.end(); ++map_it) {
		uint32_t index = map_it->first;
		NodeList &update_list = map_it->second;
		//cout << update_list.size() << " ";
		while(!update_list.empty()) {
			package.set_realfullpath(update_list.front());
			update_list.pop_front();
			if(index == selfIndex) {
				string *str;
                        	str = new string(package.SerializeAsString());
                        	pthread_mutex_lock(&notq_lock);
                        	notifyq.push(str);
                        	pthread_mutex_unlock(&notq_lock);
				//int ret = update(package);
			}
			//else if (index != toid){
			else {
				string update_str = package.SerializeAsString();
				pthread_mutex_lock(&msg_lock);
				int ret = svrclient.svrtosvr(update_str, update_str.size(), index);
				pthread_mutex_unlock(&msg_lock);
			}
		}
	} //cout << endl;
}




