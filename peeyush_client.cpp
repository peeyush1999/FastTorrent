#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include <bits/stdc++.h>
#include <fstream>
#include <math.h>
#include "sha1.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <semaphore.h>

//#include <openssl/sha.h>

//#define PORT 1234
//#define HOST "127.0.0.1"
#define SA struct sockaddr

#define SMALL_CHUNK 65000

using namespace std;
sem_t m,t,filelock;

int PORT;
string HOST="127.0.0.1";

int clientPort;

static bool keepRunning = true;

void intHandler(int) {
    keepRunning = false;
    exit(0);

}



int listenPort;

int trackerConnection; // will store tacker conn info
int activeTrackerNumber=0;



string globUsername;
ofstream myfile;

void mlog(string s)
{
    sem_wait(&m);
    myfile<<s<<endl;
    sem_post(&m);
}

set<string> notstarted;
set<string> finished; //contain sha
set<string> unfinished; //contain sha

vector< pair<string,string> > trackerInfo;// <host,port>

unordered_map<string, vector< vector<string> > > chunkinfo;

unordered_map<string, vector< pair<int , vector<string> > > > pselectioninfo;

unordered_map<string, string> downloadedFile;
unordered_map<string, string> shamap; // sha of file and file path
unordered_map<string, vector<string> > grpfileinfo;
//=================================================================
//============================Headers Ends=========================
//=================================================================
bool comp(pair<int , vector<string> > &a, pair<int , vector<string> > &b)
{
    if ((a.second).size() < (b.second).size())
        return true;

    return false;
}

//#define DEBUG false
//===============File Transfer Header=====================
#define CHUNK_SIZE (512*1024)

string get_chunk_sha(string name, int chunk_num); //return the element in chunk
string sha1(string s); //return sha1 of string
void share_file(string name);// share file and make .torrent file
long long int valid_chunk(string name, int chunk_num); //validate chink
string get_chunk_sha(string name, int chunk_num); //get chunk sha
pair<char*, unsigned long long int> get_chunk(string name, int chunk_num); // return chunk in string
string newsha1(char *str);

void handleClient(int sock, struct sockaddr_in cli);
//===============File transfer header ends================

//It will download all file present in un finished and notstarted

void receive_file(string destination, int desc,int chunknum) {

    sem_wait(&filelock);
    char buff[CHUNK_SIZE];//adding offset for flag;
    unsigned long long int mark=18446744073709551615;

    string nn=destination+to_string(chunknum);

    FILE* psudo = fopen(&destination[0], "a");
    fclose(psudo);


    FILE* dest = fopen(&destination[0], "r+");
    //FILE* dest12 = fopen(&nn[0], "w");
    fseek(dest, 0, SEEK_SET); // making pointer to zero


    fseek(dest, CHUNK_SIZE*chunknum, SEEK_SET);
    size_t size;

    while ((size = recv(desc, buff, SMALL_CHUNK, 0)) > 0) 
    {   
        if(size==mark)
            break;

        long long int k = fwrite(buff, 1, size, dest);
        //fwrite(buff, 1, size, dest);
        memset(buff, 0, sizeof(buff));
        mlog("\nreceive_file: size read "+to_string(size)+" "+to_string(k)+"\n");
    }
    fclose(dest);

    sem_post(&filelock);
    return;
}

void sendFileChunks( int desc, char *buff,long long int chunksize) {
    
    /*char buff[CHUNK_SIZE];
    FILE* src = fopen(&path[0], "r");
    size_t size;
    
    // copying the contents
    size_t read_size;
    read_size = chunksize;

    if ((size = fread(buff, 1, read_size, src)) > 0) 
    {
        if(chunksize < CHUNK_SIZE)
            buff[CHUNK_SIZE]='\0';

        //adding header and footer
        string newData(buff);
        send(desc, &newData[0], size, 0);

        //send(desc, buff, size+6, 0);

        memset(buff, 0, sizeof(buff));
        mlog("sendFilechunk: size read "+to_string(size));
    }
    
    fclose(src);
    */

     

    long long int i=0;

    while(i<chunksize)
    {   
        int k;

        char smallBuff[SMALL_CHUNK]={0};
        for(k=0;k<SMALL_CHUNK;k++ )
        {
            smallBuff[k]=buff[i];
            i++;
            if(i>chunksize)
                break;
        }
        
        long long int t = send(desc, smallBuff, k, 0);    
        mlog("sendFilechunk: size sent "+to_string(t) + " " + to_string(k));

    }


    
    free(buff);

}



//================================================================
//================================================================

//================================================================
//================Functions Declaration===========================
//================================================================
//return conn socket
int getconsock( string host, string port)
{
    mlog("#Inside getconsock connection");
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
    // socket create and varification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        mlog("#getconsock : socket creation failed");
        printf("socket creation failed...\n");
        return -1;
    }
    else
    {
        mlog("#getconsock : socket creation success");
        printf("Socket successfully created..\n");
    }


    // assign IP, PORT

    mlog("#getconsock: before assignment "+host+" "+port);
    char hst[64];
    strcpy(hst,host.c_str());

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(hst);
    servaddr.sin_port = htons(stoi(port));

    mlog("#getconsock : Assigned host port");
    // connect the client socket to server socket
    if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
        printf("connection with the server failed...\n");
        mlog("#getconsock : connection with other peer failed");
        return -1;
    }
    else
    {   
        mlog("#getconsock : connection success");
        printf("connected to the server..\n");

        mlog("#gconsock: function ends return "+ to_string(sockfd));

        return sockfd;
        
    }
}




//------obselete-----------
string sha1(string s)
{

    SHA1 checksum;
    checksum.update(s);
    return checksum.final();

}
//------------------------
string& getFile(string name) {
    string *s = new std::string;
    s->reserve(1024);
    fstream fp;


    fp.open(name, std::ios::in);
    if (!(fp.is_open())) {
        fprintf(stderr, "Unable to open the file\n");
        exit(EXIT_FAILURE);
    }
    else {
        std::string line;
        while (fp >> line) {
            s->append(line);
        }
    }
    fp.close();
    return *s;
}





string newsha1(char *str)
{
    string s(str);
    SHA1 checksum;
    checksum.update(s);
    return checksum.final();

}

void share_file(string name)
{
    ifstream in(name);

    if (in.fail())
    {
    #ifdef DEBUG
            cout << "share_file : Failed to load File." << endl;
    #endif
        }

        long long int size = CHUNK_SIZE;

        streampos beg, end;
        beg = in.tellg();
        in.seekg(0, ios::end);
        end = in.tellg();
        in.seekg(0, ios::beg);
        in.close();
        long long int file_size  = end - beg;
        long long int tot_chunk = ceil( (float)file_size / CHUNK_SIZE);

    #ifdef DEBUG
        cout << "share_file :" << " File Name :" << name << " File Size :" << file_size << " Tot Chunk :" << tot_chunk << endl;
    #endif
        int i;

        string s;
        for (int i = 0; i < tot_chunk; i++)
        {
            s += get_chunk_sha(name, i);
            cout << "\n--->" << get_chunk_sha(name, i) << endl;
        }

    #ifdef DEBUG
        cout << "Hash : " << s << endl;
    #endif

        ofstream out(name + ".torrent");

        if (out.fail())
        {
    #ifdef DEBUG
            cout << "Mtorrent creation failed" << endl;
    #endif
    }
    out << "tracker1\n" << "tracker2\n" << name + "\n" << size << "\n" << s;
    out.close();
}

long long int tot_chunk(string name)
{
    ifstream in(name);

    if (in.fail())
    {

    }

    long long int size = CHUNK_SIZE;

    streampos beg, end;
    beg = in.tellg();
    in.seekg(0, ios::end);
    end = in.tellg();
    in.seekg(0, ios::beg);
    in.close();
    long long int file_size  = end - beg;
    long long int tot_chunk = ceil( (float)file_size / CHUNK_SIZE);
    return tot_chunk;
}


long long int valid_chunk(string name, int chunk_num)
{
    ifstream in(name);

    if (in.fail())
    {

    }

    long long int size = CHUNK_SIZE;

    streampos beg, end;
    beg = in.tellg();
    in.seekg(0, ios::end);
    end = in.tellg();
    in.seekg(0, ios::beg);
    long long int file_size  = end - beg;
    long long int tot_chunk = ceil( (float)file_size / CHUNK_SIZE);
    in.close();


    if (chunk_num >= tot_chunk)
        return -1;

    if (chunk_num == tot_chunk - 1)
        size = file_size - CHUNK_SIZE * chunk_num;

    cout << "Valid chunk :  " << size << endl;
    return size;
}

string get_chunk_sha(string name, int chunk_num)
{
    long long int size = valid_chunk(name, chunk_num);

    if (size >= 0)
    {

        ifstream in(name);
        in.seekg(CHUNK_SIZE * chunk_num);

        //char *buff = (char *)malloc(size * sizeof(char));
        char buff[CHUNK_SIZE];
        in.read(buff, size);
        in.close();

        return newsha1(buff);

    }
    else
    {
        return "-1";
    }

}
/*
pair<char*, unsigned long long int> get_chunk(string name, int chunk_num) // it will return the pointer to the memory where chunk is loaded and size of chunk
{
    cout << ">>get chunk : ";
    long long int size = valid_chunk(name, chunk_num);
    cout << "get_chunk chunk :  " << size << endl;

    ifstream in(name);
    //ofstream out(name+to_string(chunk_num));
    char *buff = (char*)malloc(size);
    //char buff[size];
    memset(buff, 0, size * sizeof(char));
    cout << "Size allocated : " << size << endl;

    if (size >= 0)
    {
        in.seekg(CHUNK_SIZE * chunk_num);
        in.read(buff, size);

        /*if (size < CHUNK_SIZE)
        {
            buff[size] = '\0';
        }
        */
        //out<<buff;
    /*}
    else
    {
        
        return {buff, 0};
    }
    //out.close();

    return {buff, size};
}
*/

pair<char*, unsigned long long int> get_chunk(string name, int chunk_num) // it will return the pointer to the memory where chunk is loaded and size of chunk
{
    cout << ">>get chunk : ";
    long long int size = valid_chunk(name, chunk_num);
    cout << "get_chunk chunk :  " << size << endl;
    mlog("getChunk :"+name);
    //ifstream in(name);
    
    FILE *dest = fopen(&name[0],"r");

    //ofstream out(name+to_string(chunk_num));
    char *buff = (char*)malloc(size);
    //char buff[size];
    memset(buff, 0, size * sizeof(char));
    cout << "Size allocated : " << size << endl;


    if (size >= 0)
    {
        //in.seekg(CHUNK_SIZE * chunk_num);
        //in.read(buff, size);
        fseek(dest,CHUNK_SIZE * chunk_num,SEEK_SET);
        fread(buff,size,1,dest);

        /*if (size < CHUNK_SIZE)
        {
            buff[size] = '\0';
        }
        */
        //out<<buff;
    }
    else
    {
        
        return {buff, 0};
    }
    //out.close();

    return {buff, size};
}


pair<char*, unsigned long long int> get_chunk_size_provided(string name, int chunk_num, long long int size) // it will return the pointer to the memory where chunk is loaded and size of chunk
{
    cout << ">>get chunk : ";
    //long long int size = valid_chunk(name, chunk_num);
    cout << "get_chunk chunk :  " << size << endl;

    //ifstream in(name);
    
    FILE *dest = fopen(&name[0],"r");

    //ofstream out(name+to_string(chunk_num));
    char *buff = (char*)malloc(size);
    //char buff[size];
    memset(buff, 0, size * sizeof(char));
    cout << "Size allocated : " << size << endl;


    if (size >= 0)
    {
        //in.seekg(CHUNK_SIZE * chunk_num);
        //in.read(buff, size);
        fseek(dest,CHUNK_SIZE * chunk_num,SEEK_SET);
        fread(buff,size,1,dest);

        /*if (size < CHUNK_SIZE)
        {
            buff[size] = '\0';
        }
        */
        //out<<buff;
    }
    else
    {
        
        return {buff, 0};
    }
    //out.close();

    return {buff, size};
}

string get_chunk_into_file(string name, int chunk_num) // it will return the pointer to the memory where chunk is loaded and size of chunk
{

    cout << ">>get chunk : ";
    long long int size = valid_chunk(name, chunk_num);
    cout << "get_chunk chunk :  " << size << endl;


    mlog("get_chunk_into file called "+name+to_string(chunk_num)+" "+to_string(size));
    ifstream in(name);
    ofstream out(name+to_string(chunk_num), ios::out | ios::trunc);
    
    in.seekg(CHUNK_SIZE * chunk_num);
    char ch;

    while(size--)
    {
        in.get(ch);
        out<<ch;
    }
    out.close();

    return name+to_string(chunk_num);
}


//function will communicate with the tracker......
//This function will enquire all peers for chunks they have after that it will allocate chunk number with peers ip ans 
//sort it with the chunks with least peers. and put entry into nostarted map.
void downloadFile(string sha, vector<string> peers,string address)
{
    mlog("Inside Download File ::::");

    

    mlog("##Number of Peers :"+to_string(peers.size()));

    for (string ipport : peers)
    {
        mlog("Peers IP :"+ipport);
    }

    for (string ipport : peers)
    {

        int idx = ipport.find_last_of(":");
        string port = ipport.substr(idx + 1);
        string ip = ipport.substr(0, idx);

        cout << ip << "  (-^-)  " << port << endl;
        mlog(ip +   "(-^-)" + port  );






        int sockfd, connfd;
        struct sockaddr_in servaddr;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            cout << "Socket creation failed!!!";
        }
        
        servaddr.sin_family = AF_INET;

        servaddr.sin_addr.s_addr = inet_addr(&ip[0]);
        servaddr.sin_port = htons(stoi(port));

        if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
            printf("connection with the server failed...\n");
            mlog("Connection With Peer Failed!!!");
        }
        else
        {   
            mlog("connected to the peer "+ipport);
            //get chunk info---------
            char buff[SMALL_CHUNK];
            string cmd = "CHUNKS " + sha;
            strcpy(buff, cmd.c_str());
            write(sockfd, buff, sizeof(buff));
            mlog(">>Wroted CHUNKS SHA ");
            if (read(sockfd, buff, sizeof(buff)) == 0)
            {
                mlog(">>Connection closed Abruptly Inside download file!!!");
                close(sockfd);
                continue;
            }            
        
            string chunkno(buff);
            mlog(">>Received the data, Closing Connection "+chunkno);

            close(sockfd);
            mlog(">>Closed socket");
            


            string tmp = "";
            for (char ch : chunkno)
            {
                if (ch == ' ' )
                {
                    if(tmp!="")
                        {
                            mlog(">>" + sha +" "+tmp +" "+ ipport);
                            if(chunkinfo[sha][stoi(tmp)].empty())
                                chunkinfo[sha][stoi(tmp)].push_back(ipport);
                            else if(chunkinfo[sha][stoi(tmp)][0]!="FIN")
                                chunkinfo[sha][stoi(tmp)].push_back(ipport);

                        }

                    tmp = "";
                    continue;
                }
                tmp = tmp + ch;
            }
            mlog(">>Alloted IPPORT to chunk ");

        }     
    }

        mlog(">>Reached CheckPoint");
        for (auto it : chunkinfo)
            {
                cout << it.first << "  #:  " << endl;
                mlog(it.first+"  #:  \n");
                int i = 0;
                for (auto v : it.second)
                {
                    myfile<<"\nchunk "+to_string(i)+": ";

                    cout << "chunk " << i << ":";
                    for (string s : v)
                    {   
                        myfile<<" "<<s;
                        cout << " " << s;
                    }
                    cout << endl;
                    i++;
                }
                cout << endl;
            }

        sem_wait(&t);
        notstarted.insert(sha);
        
        mlog(">>Implementing Pselection");
        int len=chunkinfo[sha].size();
        vector< pair<int, vector<string> > > v;
        pselectioninfo[sha]=v;

        for(int i=0;i<len;i++)
        {
            pselectioninfo[sha].push_back({i,chunkinfo[sha][i]});
        }


        sort(pselectioninfo[sha].begin(), pselectioninfo[sha].end(), comp);
        int cnt = 0;
        for (auto row : pselectioninfo[sha])
        {
            myfile<<row.first<<" : ";
            cout << row.first << " : ";
            for (string ss : row.second)
            {
                myfile<<ss<<" ";
                cout << ss;
            }
            myfile<<'\n';
            cout << endl;
            cnt++;
        }
        mlog(">>Implemented Pselection completed");

        sem_post(&t);
        mlog(">>ENtry Added and downloadfile() completed");

}



//
void handleSocket(int sock)
{
    trackerConnection = sock;
    string cmd;
    char buff[1024];
    while (keepRunning)
    {
        //cin>>buff; // space break
        buff[0] = '\0';
        getline(cin, cmd);
        
        //print all chunks info if chunk present the FIN else peer containg that chunk.
        if (cmd.substr(0, 6) == "upfile")
        {
            for (auto it : chunkinfo)
            {
                mlog(it.first+" : ");
                cout << it.first << "  #:  " << endl;
                int i = 0;
                for (auto v : it.second)
                {
                    myfile<<"Chunk "<<i<<":";
                    cout << "chunk " << i << ":";
                    for (string s : v)
                    {
                        myfile<<" "<<s;
                        cout << " " << s;
                    }
                    myfile<<"\n";
                    cout << endl;
                    i++;
                }
                cout << endl;
            }
            continue;
        }
        else if (cmd.substr(0, 14) == "show_downloads")
        {
            for (string it : finished)
            {   
                mlog("Downloaded File :");
                mlog(it);
                string sha=it;
                cout<<"[C]";
                for(string ss : grpfileinfo[sha])
                {
                    cout<<" "<<ss;
                }
                cout<<shamap[sha]<<endl;
                
            }
            for (string it : unfinished)
            {   
                mlog("Downloaded File :");
                mlog(it);
                string sha=it;
                cout<<"[D]";
                for(string ss : grpfileinfo[sha])
                {
                    cout<<" "<<ss;
                }
                cout<<shamap[sha]<<endl;
                
            }
            for (string it : notstarted)
            {   
                mlog("Downloaded File :");
                mlog(it);
                string sha=it;
                cout<<"[D]";
                for(string ss : grpfileinfo[sha])
                {
                    cout<<" "<<ss;
                }
                cout<<shamap[sha]<<endl;
                
            }
            continue;
        }

        //if cmd download_file
        //it will receive sha and peer info containing that file
        //call the download file function with req info
        if (cmd.substr(0, 13) == "download_file")
        {
            //sending command to server-----------
            strcpy(buff, cmd.c_str());
            write(sock, buff, sizeof(buff));

            //Fetchin arguments from command-----------
            int flag = 0;
            string str = cmd.substr(14, cmd.size() - 14);
            string filename = "";
            string grpid = "";
            string destpath = "";
            for (char ch : str)
            {
                if (ch == ' ')
                {
                    flag++;
                    continue;
                }
                if (flag == 0)
                    grpid = grpid + ch;
                if (flag == 1)
                    filename = filename + ch;
                if (flag == 2)
                    destpath = destpath + ch;
            }
            cout << grpid << " " << filename << " " << destpath << endl;

            //Getting back SHA and IP:PORT info-----------
            if (read(sock, buff, sizeof(buff)) == 0)
            {
                mlog("Connection closed Abruptly download file!!!");
            }
            cout << buff << endl;

            string portinfo(buff);
            mlog("Clients with File:" + portinfo);
            flag = 0;
            string reqsha = "";
            string tmp = "";
            
            //generating list with individual peer, seperator is '\n'-----------
            
            vector<string> peersinfo;
            for (char ch : portinfo)
            {
                if (ch == '\n')
                {
                    if (flag == 1)
                    {   cout << tmp << "-----@@@@@---------" << endl;
                        peersinfo.push_back(tmp);
                        tmp = "";
                    }
                    flag = 1;
                    continue;
                }
                if (flag == 0)
                    reqsha += ch;
                if (flag == 1)
                    tmp += ch;
            }
            cout << reqsha << "-----@@@@@---------" << endl;


            grpfileinfo[reqsha].push_back(grpid);//adding group id from where i downloaded
            

            shamap[reqsha]=destpath + "/" +filename;//stroing where while need to be
            mlog("@@@File will be stored Here : "+shamap[reqsha]);

            int numChunk = stoi(reqsha.substr(40));

            if (chunkinfo.find(reqsha) == chunkinfo.end())
            {   vector<vector<string>> v(numChunk, vector<string> {});
                chunkinfo[reqsha] = v;
            }
            //making entry for downloadable file if new in not strated else downlaoded
            mlog("reached checkpoint 3");
            downloadFile(reqsha, peersinfo,destpath);

        }
        else
        {

            // if cmd  contains login appending ip:port of client
            if (cmd.substr(0, 5) == "login")
            {
                string s=cmd.substr(6);
                string username="";
                for(char ch : s)
                {
                    if(ch==' ')
                    {
                        break;
                    }
                    username+=ch;
                }
                globUsername = username;
                cmd = cmd + " " + HOST + ":" + to_string(listenPort);
            }

            //if user is uploading complete file------
            if (cmd.substr(0, 11) == "upload_file")
            {
                string str = cmd.substr(12, cmd.size() - 12);
                string filepath = "";
                string grpid = "";
                int flag = 0;
                for (char ch : str)
                {
                    if (ch == ' ')
                    {
                        flag++;
                        continue;
                    }
                    if (flag == 0)
                        filepath = filepath + ch;
                    if (flag == 1)
                        grpid = grpid + ch;
                }

                string *buffer = &(getFile(filepath));
                //cout<<*buffer;
                string filesha = sha1(*buffer);
                cmd = cmd + " " + filesha + to_string(tot_chunk(filepath));

                //updating sha+numchunk mapping
                shamap[filesha + to_string(tot_chunk(filepath))]=filepath;

                grpfileinfo[filesha + to_string(tot_chunk(filepath))].push_back(grpid);// updating grp id

                int numChunk = tot_chunk(filepath);
                vector<vector<string>> v(numChunk, vector<string> {"FIN"});

                filesha = filesha + to_string(tot_chunk(filepath)); //appending bunber
                
                //making new entry in completed file
                finished.insert(filesha);

                chunkinfo[filesha] = v;
                delete buffer;
            }


            strcpy(buff, cmd.c_str());

            write(sock, buff, sizeof(buff));

            if (read(sock, buff, sizeof(buff)) == 0)
            {
                mlog("Connection closed Abruptly login upload !!! Trying to connect with Other Tracker");
                cout<<"Connection closed Abruptly login upload !!! Trying to connect with Other Tracker"<<endl;
                sock=-1;

                while(sock<0)
                {

                    activeTrackerNumber = 1 - activeTrackerNumber;

                    sock = getconsock(trackerInfo[activeTrackerNumber].first,trackerInfo[activeTrackerNumber].second);
                    if(sock<0)
                        {
                            cout<<"Connection failed"<<endl;
                            sock=-1;
                        }
                    else
                        cout<<"Enter Previous Command Again!!! Connection reestablished with tracker num "<<activeTrackerNumber<<endl;

                    sleep(3);
                }

            }
            cout << "-------" << endl;
            cout << buff << endl;
            cout << "---$---" << endl;
        }
    }

}





//-----------------------------------------------------------------
//-------------To communicate with clients--------------------
//-----------------------------------------------------------------


// will open port so that other peers can connect with it....
void litsenport()
{
    mlog("============Litsen Port Thread Started==============");
    int sockfd, connfd, len;
    struct sockaddr_in servaddr, cli, my_addr;

    // socket create and verification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        
    }
    else
        printf("For Peer Socket successfully created..\n");


    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(&HOST[0]); //inet_addr(HOST);
    servaddr.sin_port = htons(clientPort);// binding with zero port os will find one free for us

    int enable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0);

    // Binding newly created socket to given IP and verification
    if ((bind(sockfd, (sockaddr* )&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed...\n");
        
    }
    else
        printf("Socket successfully binded..\n");




    if ((listen(sockfd, 50)) != 0) {
        printf("Listen failed...\n");
        
    }
    else
    {
        printf("Port for listening is ready..\n");
        bzero(&my_addr, sizeof(my_addr));
        socklen_t len = sizeof(my_addr);
        getsockname(sockfd, (struct sockaddr *) &my_addr, &len);
        unsigned int myPort = ntohs(my_addr.sin_port);
        cout << myPort << "  listening here" << endl;
        listenPort = myPort;
    }

    //socklen_t lenn;

    socklen_t lenn = sizeof(cli);



    // Accept the data packet from client and verification
    while (keepRunning)
    {
        connfd = accept(sockfd, (sockaddr*)&cli , &lenn);
        cout << "new conn request!!!!!!!" << connfd << endl;
        if (connfd < 0) {
            printf("server acccept failed...\n");
        }
        else
        {
            printf("server acccept the client...\n");
            cout << "IP Connected : " << inet_ntoa(cli.sin_addr) << endl; // conver int address to ip format
            cout << "Port Connected : " << cli.sin_port << endl;


            thread t1(handleClient, connfd, cli);
            t1.detach();
        }


        //cout<<"Client request completed!!!\n";


    }
    close(sockfd);
    // After chatting close the socket
    mlog("============Conn CLosed : Litsen Port Thread End==============");
}


//returns the size of file;
long long int get_file_size(string name)
{
    ifstream in(name);
    streampos beg, end;
    beg = in.tellg();
    in.seekg(0, ios::end);
    end = in.tellg();
    in.seekg(0, ios::beg);
    in.close();
    long long int file_size  = end - beg;
    return file_size;
}

void handleClient(int sock, struct sockaddr_in cli)
{
    char buff[8192];

    int login = 0;

    //here client acting as server after getting request from other client----

    buff[0] = '\0';
    
    if (read(sock, buff, sizeof(buff)) == 0)
        mlog("HandleClient :Client Terminated Abruptly !!!");
    

    cout << "server received :" << buff << endl;

    string s(buff);

    //test message not using in implementation...

    if (s.substr(0, 5) == "HELLO")
    {
        string msg = "Hello There!!!";
        strcpy(buff, msg.c_str());
        send(sock, buff, sizeof(buff), 0);
    }
    //It returns the info chunks that are present for the file requested format chunk, sha--- 
    else if (s.substr(0, 6) == "CHUNKS")
    {
        mlog("HandleClient : Received CHUNKS");
        string filesha = s.substr(7, s.size() - 7);

        string msg = "";
        //generating message containing number of chunks----
        
        for (auto it : chunkinfo)
        {
            if (it.first == filesha)
            {
                cout << it.first << " :" << endl;
                int i = 0;
                for (auto v : it.second)
                {
                    cout << "chunk " << i << ":";
                    for (string s : v)
                    {

                        cout << " " << s;
                        if(s=="FIN")                        
                            {
                                msg = msg + to_string(i) + " ";
                                break;
                            }

                    }
                    cout << endl;
                    i++;
                }
                cout << endl;
            }
        }
        
        //sending response to client===
        cout << "Chunks I have : " << msg << endl;
        mlog("HandleClient : chunks I have : "+msg);
        strcpy(buff, msg.c_str());
        send(sock, buff, sizeof(buff), 0);

    }
    //will return the chunk of file sha of chunk and chunk data
    else if (s.substr(0, 9) == "FILECHUNK") //FILECHUNK SHA NUM
    {
        mlog("HandleClient :File chunk commnad");

        mlog("#filechunk : command entry");
        string str = s.substr(10, s.size() - 10);
        string filesha = "";
        string chunknum = "";
        int flag = 0;
        for (char ch : str)
        {
            if (ch == ' ')
            {
                flag++;
                continue;
            }
            if (flag == 0)
                filesha = filesha + ch;
            if (flag == 1)
                chunknum = chunknum + ch;
        }

    
        string filename=shamap[filesha];
        mlog("#filechunk : file name from sha "+filesha+" filename : "+filename);
        
        pair<char*, unsigned long long int> tmp=get_chunk(filename, stoi(chunknum));
        string chunkSha = newsha1(tmp.first); // sending first part of tmp which contains our data

        //string temporary_file = get_chunk_into_file(filename, stoi(chunknum));
        /*        
        long long int tsize=get_file_size(temporary_file);
        string chunkSize = to_string(tsize);//getting chunk size which will be transmitted.
        mlog("#filechunk : got chunk "+chunknum+"  size : "+chunkSize);

        string *buffer = &(getFile(temporary_file));
        string chunkSha = sha1(*buffer);
        delete buffer;

        string tosend = chunkSha+" "+chunkSize;
        buff[0]='\n';
        */

        
        string chunkSize = to_string(tmp.second);
        string tosend = chunkSha+" "+chunkSize;
        strcpy(buff, tosend.c_str());
        int num1 = send(sock, buff, sizeof(buff), 0);

        mlog("#filechunk: sha of new chunk "+tosend);
        mlog("#filechunk: tosend resp size"+to_string(num1));
        

        /*
        buff[0] = '\0';
        if (read(sock, buff, sizeof(buff)) == 0)
            mlog("#filechunk : client abruplty Terminated unable to read sha ack.");
        
        cout<<"********************"<<buff;
        string mm(buff);
        mlog("#filechunk : received after sending "+mm);
        */

        
        
        sendFileChunks(sock,tmp.first,tmp.second);
        //send_file(sock,tmp.first,tmp.second);


        
        /*buff[0] = '\0';
        if (read(sock, buff, sizeof(buff)) == 0)
            mlog("#filechunk : client abruplty Terminated unable to read sha ack.");
        */
        
        mlog("#filechunk received final goodbye");
        

        if (read(sock, buff, sizeof(buff)) == 0)
            mlog("#filechunk : client abruplty Terminated unable to read sha ack.");

    }
    else
    {
        string msg = "Make new request with valid command!!!";
        strcpy(buff, msg.c_str());
        send(sock, buff, sizeof(buff), 0);

    }
    
    //  }
    close(sock);

}
//================================================================
//================Functions Declaration Ends======================
//================================================================

pair<string, string> separateIP(string s)
{
    int flag=0;
    string host;
    string port;

    for(char ch:s)
    {
        if(ch==':')
        {
            flag++;
            continue;
        }
        if(flag==0)
            host+=ch;
        else
            port+=ch;
    }
    return{host,port};

}


void updateTracker(string sha)
{
    
    string allgrp="";
    char buff[4096];
    string port=to_string(PORT);
    int sock = getconsock(HOST,port);
    if(sock<0)
    {
        mlog("@Unable to connect with peer for update");
        sock=-1;
        while(sock<0)
                {

                    activeTrackerNumber = 1 - activeTrackerNumber;

                    sock = getconsock(trackerInfo[activeTrackerNumber].first,trackerInfo[activeTrackerNumber].second);
                    if(sock<0)
                        {
                            cout<<"Connection failed"<<endl;
                            sock=-1;
                        }
                    else
                        {
                            cout<<"Enter Previous Command Again!!! Connection reestablished with tracker num "<<activeTrackerNumber<<endl;
                            break;
                        }

                    sleep(3);
                }
    }
     
        for(string s: grpfileinfo[sha])
        {
            allgrp=allgrp+s+" "; //generate all group 
        }
        string cmd = "IHAVE "+sha+" "+globUsername+" "+allgrp;
        mlog("updateTracker : command sent "+cmd);
        
        strcpy(buff, cmd.c_str());
        write(sock, buff, sizeof(buff));
    
        notstarted.erase(notstarted.find(sha));
        unfinished.insert(sha);

        close(sock);
    

}

void downloadThread(int sock,string sha, int chunknum)
{
 

   
        mlog("$Inside DownloadThread funnction---------");
        char buff[SMALL_CHUNK];
        string cmd="FILECHUNK "+sha+" "+to_string(chunknum);
        mlog("$cmd "+cmd);
        
        strcpy(buff, cmd.c_str());
        send(sock, buff, sizeof(buff), 0);
        memset(buff, 0, sizeof(buff));



        mlog("$DownloadThread : Sent reuqest for FILECHUNK sha chunk num---------");
        if (read(sock, buff, sizeof(buff)) == 0)
            {
                mlog("$read fail : "+sha+" "+to_string(chunknum)+" ");
                cout<<"read fail : "<<sha<<" "<<chunknum<<" "<<endl;
            }
        else
        {
            mlog("$Read successful");
        }
        

        string rdata(buff);
        string chunkSha=rdata.substr(0,40);
        string sizeToRead = rdata.substr(41);
        long long int tsize=stoll(sizeToRead);

        mlog("DownloadThread : Got SHA of chunk---------"+chunkSha+" Size of upcoming file "+sizeToRead);
        
        //---------------Response to sha + size--------------------
        /*cmd="OK";
        strcpy(buff, cmd.c_str());
        send(sock, buff, sizeof(buff), 0);
        mlog("DownloadThread : responded back with OK");
        */

        //----------receiving file --------------
        //ofstream out(sha+to_string(chunknum));
        //char *carray=(char *)malloc(tsize);

        char text[1024];
        size_t datasize=tsize;

        string nn=shamap[sha];

        mlog("downloadThread: "+sha+" "+nn);

        //nn="1"+nn;
        
        /*for(auto it : shamap)
        {
            mlog(sha+"======"+it.first+"====="+it.second+"    ");
        }
        */
        
        receive_file(nn, sock, chunknum);
        
       

        /*cmd="OK";
        strcpy(buff, cmd.c_str());
        send(sock, buff, sizeof(buff), 0);
        */
        
        mlog("Download:Thread Download Successfull closing socket!!!");    
        close(sock);


        mlog("DownloadThread : checking file sha and provided sha matched or not");


        pair<char*, unsigned long long int> tmp=get_chunk_size_provided(nn, chunknum,tsize);
        string filesha = newsha1(tmp.first); // sending first part of tmp which contains our data


        if(chunkSha == filesha)
        {   
            
            mlog("DownloadThread : matched completed chunk saved ");
            chunkinfo[sha][chunknum].clear();
            chunkinfo[sha][chunknum].push_back("FIN");
            
            if(notstarted.find(sha)!=notstarted.end())
                updateTracker(sha);
        }
        else
        {
            mlog("^sha doesnt matched "+chunkSha+ " " + filesha);
        }
    
    mlog("DownloadThread : ENds");
}

unordered_map<string, unordered_set<string> > numthread;

void download()
{

    mlog("@-----Download Function Started ---------");

    while(keepRunning)
    {

        if(!notstarted.empty())
        {
            sem_wait(&t);
            auto it=notstarted.begin();
            sem_post(&t);

            string itsha=*it;
            cout<<"Download Thread "<< itsha <<endl; //*it points to sha
            mlog("Download Thread "+itsha);

            int numchunk = stoi(itsha.substr(40));

            int downloaded=0;
            for(int i=0;i<numchunk;i++)
            {
                pair<int,vector<string> > piece = pselectioninfo[itsha][i];
                vector<string> memlist = chunkinfo[itsha][piece.first];
                

                mlog("@--memlist size "+to_string(memlist.size()));
                mlog("@--sha chunk 0 "+ chunkinfo[itsha][piece.first][0]);
                

                //for(string s : memlist)
                //{
                int numPeer= memlist.size();
                int random = rand() % numPeer;
                mlog("Random Key from 0-"+to_string(numPeer) + " " + to_string(random));

                if(!memlist.empty() && memlist[0]!="FIN")
                {

                    string s=(piece.second)[random];
                    mlog("Random Peer for"+to_string(piece.first)+" "+s);

                    if(s=="")
                        continue;
                    
                    pair<string,string> p=separateIP(s);
                    mlog("@>>>>> "+s+" @ "+p.first + " $ "+ p.second);
                    mlog("@seperateIP finished");
                    int sock = getconsock(p.first,p.second);
                    if(sock<0)
                    {
                        mlog("@Unable to connect with peer");
                        cout<<"Unable to connect with peer !!"<<endl;
                        continue;
                    }
                    else
                    {   mlog("@Connected to Peer "+p.first+" "+p.second+"");   
                        mlog("@connected with peer forward to download ");
                        mlog("@Created Thread!!!");
                        mlog("@arg sending :"+itsha+" "+to_string(piece.first));
                        
                        
                            thread d(downloadThread,sock,itsha,piece.first);
                        
                            numthread[itsha].insert(to_string(piece.first));
                            mlog("Size of "+to_string(numthread[itsha].size()));
                            d.join();
                            
                            sleep(0.5);
                        

                        

                        break;
                    }
                        
                 } 
                 else if(!memlist.empty() && memlist[0]=="FIN")
                 {
                    downloaded++;
                 }
                //}

            }
            if(numchunk==downloaded)
            {
                unfinished.erase(unfinished.find(itsha));
                mlog("File completely downloaded!!!!!");
                finished.insert(itsha);
                        

            }

        }
        if(!unfinished.empty())
        {
            
            sem_wait(&t);
            auto it=unfinished.begin();
            sem_post(&t);

            string itsha=*it;
            cout<<"Download Thread "<< itsha <<endl; //*it points to sha
            mlog("Download Thread "+itsha);

            int numchunk = stoi(itsha.substr(40));

            int downloaded=0;
            for(int i=0;i<numchunk;i++)
            {
                pair<int,vector<string> > piece = pselectioninfo[itsha][i];
                vector<string> memlist = chunkinfo[itsha][piece.first];
                

                mlog("@--memlist size "+to_string(memlist.size()));
                mlog("@--sha chunk 0 "+ chunkinfo[itsha][piece.first][0]);
                

                //for(string s : memlist)
                //{
                int numPeer= memlist.size();
                int random = rand() % numPeer;
                mlog("Random Key from 0-"+to_string(numPeer) + " " + to_string(random));

                if(!memlist.empty() && memlist[0]!="FIN")
                {

                    string s=(piece.second)[random];
                    mlog("Random Peer for"+to_string(piece.first)+" "+s);

                    if(s=="")
                        continue;
                    
                    pair<string,string> p=separateIP(s);
                    mlog("@>>>>> "+s+" @ "+p.first + " $ "+ p.second);
                    mlog("@seperateIP finished");
                    int sock = getconsock(p.first,p.second);
                    if(sock<0)
                    {
                        mlog("@Unable to connect with peer");
                        cout<<"Unable to connect with peer !!"<<endl;
                        continue;
                    }
                    else
                    {   mlog("@Connected to Peer "+p.first+" "+p.second+"");   
                        mlog("@connected with peer forward to download ");
                        mlog("@Created Thread!!!");
                        mlog("@arg sending :"+itsha+" "+to_string(piece.first));
                        
                        
                            thread d(downloadThread,sock,itsha,piece.first);
                        
                            numthread[itsha].insert(to_string(piece.first));
                            mlog("Size of "+to_string(numthread[itsha].size()));
                            d.join();
                            
                            sleep(0.5);
                        

                        

                        break;
                    }
                        
                 } 
                 else if(!memlist.empty() && memlist[0]=="FIN")
                 {
                    downloaded++;
                 }
                //}

            }
            if(numchunk==downloaded)
            {
                unfinished.erase(unfinished.find(itsha));
                mlog("File completely downloaded!!!!!");
                finished.insert(itsha);
                        

            }


        }


    }

    mlog("-----Download Function Ended ---------");
}






int main(int argc, char *argv[])
{
    myfile.open("logClient.txt",ios::trunc);
    srand((unsigned) time(NULL));

    if(argc != 3)
    {
        cout<<"Run program with valid arguments";
        exit(0);
    }

    vector<string> cmd(argv,argv+argc);
    string trackerInfoFile(cmd[2]);
    string ipport(cmd[1]);
    pair<string,string> cp=separateIP(ipport);

    
    
    
    clientPort=stoi(cp.second);
    cout<<clientPort<<"**************"<<endl;


    ifstream in(trackerInfoFile);
    string tracker0,tracker1;
    in>>tracker0;
    in>>tracker1;

    in.close();

    pair<string,string> p=separateIP(tracker0);
    trackerInfo.push_back(p);

    trackerInfo.push_back(separateIP(tracker1));

    //tracker 0 port
    PORT=stoi(p.second);
    HOST=p.first;
    cout<<PORT<<" "<<HOST<<endl;
    

    sem_init(&m,0,1);
    sem_init(&t,0,1);
    sem_init(&filelock,0,1);

    mlog("####################################################");
    mlog("Startting Client");

    mlog("Handle Interupt Signal");
    signal(SIGINT, intHandler);

    thread listen(litsenport);
    listen.detach();

    thread bgdownload(download);
    bgdownload.detach();


/*
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
    // socket create and varification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
    }
    else
        printf("For Tracker Socket successfully created..\n");


    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(&HOST[0]);
    servaddr.sin_port = htons(PORT);

    // connect the client socket to server socket
    if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
        printf("connection with the server failed...\n");
        //exit(0);
    }
    else
    {   printf("connected to the server..\n");

        // function for chat
        thread t1(handleSocket, sockfd);

        t1.join();

        // close the socket
        
    }
  */
    int connfd;

    if( (connfd=getconsock(trackerInfo[activeTrackerNumber].first,trackerInfo[activeTrackerNumber].second))<0)
    {
        mlog("Connection With Tracker 0 failed.....");
        activeTrackerNumber=1-activeTrackerNumber;
        if( (connfd=getconsock(trackerInfo[activeTrackerNumber].first,trackerInfo[activeTrackerNumber].second))<0)
          {
             mlog("Unable To connect with any of the tracker");
          }  

    }

    thread t1(handleSocket, connfd);
    t1.join();


    cout<<"program terminated";
    int num = 9;
    cin >> num;

}