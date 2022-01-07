#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <unistd.h>
#include <thread>
#include <bits/stdc++.h>
#include <fstream> 
#include <signal.h>
#include <semaphore.h>
#include <bits/stdc++.h>


using namespace std;


#define MAX 80
//#define HOST htonl(INADDR_ANY)
//#define PORT 1234
#define SA struct sockaddr_in
sem_t m,t,filelock;




int masterTrackerSockNum=-1;
int subTrackerSockNum=-1;


int PORT;
string HOST;

static bool keepRunning = true;

void intHandler(int) {
    keepRunning = false;

}


ofstream myfile;
ofstream commandLog;




void mlog(string s)
{
    sem_wait(&m);
    myfile<<s<<endl;
    sem_post(&m);   
}

void pushCommand(string s)
{

    myfile.close();
    commandLog.open("tracker.txt",ios::app);
    //cout<<"##########33"<<s;
    commandLog<<s<<'\n';

    commandLog.close();
    myfile.open("trackersLog.txt",ios::app);
}


void sendToTracker(string s)
{
char buff[1024];

if(subTrackerSockNum != -1 )
    strcpy(buff,s.c_str());
    send(subTrackerSockNum, buff, sizeof(buff), 0);
    pushCommand(s);


}

vector< pair<string,string> > trackerInfo;



unordered_map< string, string> user; //uid , pass

unordered_map< string, string> userConn;

unordered_map< string, vector<string> > group;
unordered_map< string, unordered_set<string> > pending;
unordered_map< string,  unordered_map<string, unordered_set<string> > > filesgrp; //grpid, sha memlist
unordered_map<string, string> shaname; // store mapping of sha ans name

void receivedCommands(string s);



int getconsock(string host, string port)
{
    mlog("#Inside getconsock connection");
    int sockfd, connfd;
    
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

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(&host[0]);
    servaddr.sin_port = htons(stoi(port));

    mlog("#getconsock : Assigned host port");
    // connect the client socket to server socket
    if (connect(sockfd, (const sockaddr *)&servaddr, sizeof(servaddr)) != 0) {
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

string ipport(struct sockaddr_in cli)
{
    string ip(inet_ntoa(cli.sin_addr));
    string port(to_string(cli.sin_port));

    return ip+":"+port;
}
pair<string, string> get2arg(string s)
{
    string arg1="",arg2="";
    int i=0;
    for(char ch: s)
    {
        if(ch==' ')
        {
            i=1;
            continue;
        }
        if(i==0)
        {
            arg1=arg1+ch;
        }
        else
        {
            arg2=arg2+ch;   
        }

    }
    return {arg1,arg2};
}


void handleClient(int sock,struct sockaddr_in cli)
{
    char buff[1024];
    string username="test";   
    int login=0; 
    string connectport;
    bool kr=true;
    while(kr)
    {   

        buff[0]='\0';
        int r=read(sock,buff,sizeof(buff));
        if(r==0)
        {
            cout<<"Client Terminated Abruptly !!! "<<username<<" "<<sock<<endl;;
            userConn[username]="";

            //======================
            string s(buff);
            s="logout "+username;
            strcpy(buff,s.c_str());
            send(subTrackerSockNum, buff, sizeof(buff), 0);
            pushCommand(s);
            //cout<<buff;
            //======================

            kr=false;
            continue;
        }

        cout<<"server received :"<<r<<" "<<buff<<endl;

        string s(buff);
        myfile<<username<<" "<<s<<"\n";

        if(username=="SUBTRACKER")
        {
            cout<<"SUBTRACKER "<<s<<endl;
            receivedCommands(s);
        }

        if(s.substr(0,11)=="create_user")
        {
            int flag=0;
            string str = s.substr(12,s.size()-12);
            string username="";
            string password="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    username=username+ch;
                if(flag==1)
                    password=password+ch;
            }
            if(user.find(username) == user.end())
            {
                string msg="Successfully Registered !!";
                user.insert({username,password});
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);


                //======================
                strcpy(buff,s.c_str());
                cout<<buff<<endl;
                send(subTrackerSockNum, buff, sizeof(buff), 0);
                pushCommand(s);
                //cout<<buff;
                //======================

            }
            else
            {
                string msg="User already Registered !!";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
            }
        }
        else if(s.substr(0,5)=="login")
        {   

            int flag=0;
            string str = s.substr(6,s.size()-6);
            username="";
            string password="";
            string ip="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    username=username+ch;
                if(flag==1)
                    password=password+ch;
                if(flag==2)
                    ip=ip+ch;
            }
            cout<<username<<"-"<<password<<"-"<<ip<<endl;
            if(user.find(username)!=user.end() && user[username]==password)
            {
                //marking user logged in.
                userConn[username]=ip;
                login=1;
                string msg="Successfully logged in !!";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
                                   
                //======================
                strcpy(buff,s.c_str());
                send(subTrackerSockNum, buff, sizeof(buff), 0);
                pushCommand(s);
                //cout<<buff;
                //======================

            }
            else
            {
                string msg="Verification Failed !!";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
            }
            //cout<<">>"<<userConn[username]<<"--- "<<username<<endl;
        }
        else if(s.substr(0,6)=="logout")
        {
            userConn[username]="";
            login=0;
            string msg="Successfully Logged Out !!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);

            //======================
            s=s+" "+username;
            strcpy(buff,s.c_str());
            send(subTrackerSockNum, buff, sizeof(buff), 0);
            pushCommand(s);
            //cout<<buff;
            //======================

        }
        else if(s.substr(0,12)=="create_group")
        {

            if(!login)
            {
                string msg="Login first to create group!!";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
            }
            else
            {
                string grpid = s.substr(13,s.size()-13);

                cout<<"----$-----"<<grpid<<" "<<username<<endl;
                if(group.find(grpid)==group.end())
                {
                    group[grpid].push_back(username);
                    cout<<group[grpid][0]<<"---grp owner"<<endl;


                    string msg="Group successfully Created!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                
                    //======================
                    s=s+" "+username;
                    strcpy(buff,s.c_str());
                    send(subTrackerSockNum, buff, sizeof(buff), 0);
                    pushCommand(s);
                    //cout<<buff;
                    //======================


                }        
                else
                {
                    string msg="Group Already There Created!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);

                }
            }
        }
        else if(s.substr(0,11)=="list_groups")
        {
            
            string msg="List of groups: \n";
            for ( const auto &myPair : group ) {
                msg = msg + myPair.first + "\n";
            }
            //cout<<msg<<endl;

            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                   
        }
        else if(s.substr(0,11)=="leave_group")
        {
            string grpid = s.substr(12,s.size()-12);

            if(group.find(grpid)!=group.end())
            {
                vector<string> memList = group[grpid];
                auto it = find(memList.begin(),memList.end(),username);
                if(it!=memList.end())
                {
                    group[grpid].erase(it);

                    if(group[grpid].empty())
                    {
                        group.erase(grpid);
                    }
                    
                    string msg="You Left the group "+grpid+"!!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);

                    //======================
                    s=s+" "+username;
                    strcpy(buff,s.c_str());
                    send(subTrackerSockNum, buff, sizeof(buff), 0);
                    pushCommand(s);
                    //cout<<buff;
                    //======================
                    
                }
                else
                {
                    string msg="You are not the part of group !!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                }

            }
            else
            {

            string msg="Group Does Not Exists !!!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                
            }
               
        }
        else if(s.substr(0,10)=="join_group")
        {
            string grpid = s.substr(11,s.size()-11);

            if(group.find(grpid)!=group.end())
            {
                vector<string> memList = group[grpid];
                auto it = find(memList.begin(),memList.end(),username);
                if(it!=memList.end())
                {
                    string msg="You are already part of group!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                }
                else
                {
                    pending[grpid].insert(username);
                    string msg="Your request has been made successfully !!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);

                    //==============================================
                    s=s+" "+username;
                    strcpy(buff,s.c_str());
                    send(subTrackerSockNum, buff, sizeof(buff), 0);
                    pushCommand(s);
                    //==============================================

                }

            }
            else
            {

            string msg="Group Does Not Exists !!!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                
            }
               
        }
        else if(s.substr(0,13)=="list_requests")
        {
            string grpid = s.substr(14,s.size()-14);

            if(group.find(grpid)!=group.end())
            {
                if(!group[grpid].empty() && group[grpid][0]==username)
                {
                    
                    if(pending.find(grpid)==pending.end())
                    {
                        string msg="Curruntly No pending request present !!\n ";
                        strcpy(buff,msg.c_str());
                        send(sock, buff, sizeof(buff), 0);

                    }
                    else
                    {
                        unordered_set<string> memList = pending[grpid];
                        string msg="Pending Requests : \n ";

                        for(string s: memList)
                        {
                            msg=msg+s+" \n";
                        }
                        strcpy(buff,msg.c_str());
                        send(sock, buff, sizeof(buff), 0);
                    }
                }
                else
                {
                    string msg="You Are not the admin of the group !!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                }
            }
            else
            {

            string msg="Group Does Not Exists !!!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                
            }
               
        }
        else if(s.substr(0,14)=="accept_request")
        {
            pair<string, string> arg = get2arg(s.substr(15,s.size()-15));
            string grpid=arg.first;
            string userid=arg.second;

            if(pending.find(grpid)!=pending.end())
            {
                if(!group[grpid].empty() && group[grpid][0]==username)
                {
                    unordered_set<string> memList = pending[grpid];

                    if(memList.find(userid)!=memList.end())
                    {
                        group[grpid].push_back(userid);
                        pending[grpid].erase(userid);
                        
                        if(pending[grpid].empty())
                        {
                            if(pending.erase(grpid));
                                //cout<<"key found erasing";

                        
                        }
                        
                        string msg="You accepted the request of "+userid+" for group "+grpid+"\n";

                        strcpy(buff,msg.c_str());
                        send(sock, buff, sizeof(buff), 0);

                        //======================
                        s=s+" "+username;
                        strcpy(buff,s.c_str());
                        send(subTrackerSockNum, buff, sizeof(buff), 0);
                        pushCommand(s);
                        //cout<<buff;
                        //======================
                        
                    }
                    else
                    {
                        string msg="No request from selected user!!";
                        strcpy(buff,msg.c_str());
                        send(sock, buff, sizeof(buff), 0);
                    }
                }
                else
                {
                    string msg="You Are not the admin of the group !!!";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                }
            }
            else
            {

            string msg="Group Does Not Exists !!!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                
            }
               
        }
        else if(s.substr(0,8)=="userconn")
        {
            
            string msg="List of Connections: \n";
            for ( const auto &myPair : userConn ) {
                msg = msg + myPair.first+" "+myPair.second + "\n";
            }
            cout<<msg<<endl;
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
                   
        }
        else if(s.substr(0,7)=="grpinfo")
        {
            
            string msg="List of Grp with members: \n";

            for(auto it : group)
            {
                msg=msg + it.first + " : ";

                for(string st:it.second)
                {
                    msg=msg + st + " ";
                }
                msg=msg+"\n";
            }
            
            cout<<msg<<endl;
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
        }
        else if(s.substr(0,11)=="upload_file")
        {
            int flag=0;
            string str = s.substr(12,s.size()-12);
            string filepath="";
            string grpid="";
            string sha="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    filepath=filepath+ch;
                if(flag==1)
                    grpid=grpid+ch;
                if(flag==2)
                    sha=sha+ch;
            }
            cout<<filepath<<" "<<grpid<<" "<<sha<<endl;

            size_t found=filepath.find_last_of('/');

            if(found!=string::npos)
                filepath=filepath.substr(found+1);

            if(group.find(grpid)==group.end())
            {
                string msg="Group Not found!!!\n";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);

            }
            else
            {   
                vector<string> memlist=group[grpid];

                if(find(memlist.begin(),memlist.end(),username)==memlist.end())
                {
                    string msg="Only Member of Group Can Share File!!!\n";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);
                }
                else
                {

                    if(shaname.find(sha)==shaname.end())
                    {
                        string fname;

                        if(filepath[0]=='.' || filepath[0]=='~')
                            fname=filepath.substr(filepath.find_last_of("/\\") + 1);
                        else
                            fname=filepath;
                        
                        cout<<"File name "<<filepath<<endl;
                        shaname[sha]=fname;

                    }
                    
                    filesgrp[grpid][sha].insert(username);

                    string msg="File info updated successfully \n";
                    strcpy(buff,msg.c_str());
                    send(sock, buff, sizeof(buff), 0);


                    //======================
                    s=s+" "+username;
                    strcpy(buff,s.c_str());
                    send(subTrackerSockNum, buff, sizeof(buff), 0);
                    pushCommand(s);
                    //cout<<buff;
                    //======================
                }
            }

            
        }
        else if(s.substr(0,10)=="list_files")
        {
            string grpid = s.substr(11,s.size()-11);
            
            if(filesgrp.find(grpid)==filesgrp.end())
            {
                string msg="Group Not Found!!!\n";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);

            }
            else
            {
                string msg="List of files with Grp: \n";
                for(auto st:filesgrp[grpid])
                {
                    msg=msg + shaname[st.first] + "\n";
                }
                msg=msg+"\n";

                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
            }
            
        }
        else if(s.substr(0,13)=="download_file")
        {
            int flag=0;
            string str = s.substr(14,s.size()-14);
            string filename="";
            string grpid="";
            string destpath="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                     grpid=grpid+ch;
                if(flag==1)
                    filename=filename+ch;
                if(flag==2)
                    destpath=destpath+ch;
            }
            cout<<grpid<<" "<<filename<<" "<<destpath<<endl;



            if(filesgrp.find(grpid)==filesgrp.end())
            {
                string msg="Group Not Found!!!\n";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);

            }
            else
            {
                int filefound=0;
                unordered_set<string> mem;
                string msg="";
                string shaval="";

                for(auto st:filesgrp[grpid])
                {
                    if(shaname[st.first]==filename)
                    {

                        filefound=1;
                        mem=st.second;
                        shaval=st.first;
                        break;
                    }    
                }

                for(string uid : mem)
                {
                    if(userConn[uid]!="" && uid!=username)
                    {
                        msg=msg+userConn[uid]+"\n";
                    }
                }
                msg=shaval+'\n'+msg;
                cout<<msg<<endl;

                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
            }

        
        }
        else if(s.substr(0,8)=="fileinfo")
        {
            
            string msg="List of Grp with files: \n";

            for(auto it : filesgrp)
            {
                msg=msg +"->"+it.first + "\n";

                for(auto st:it.second)
                {
                    msg=msg +"|---->"+st.first +"   ("+ shaname[st.first] + ") \n";
                    for(string mem:st.second)
                        {
                            msg=msg +"    |---->"+mem + "\n ";
                        }
                }
                msg=msg+"\n";
            }
            
            cout<<msg<<endl;
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);
        }
        else if(s.substr(0,5)=="IHAVE")
        {
            string cmd = s.substr(6,s.size()-6);
            vector<string> gname;
            int flag=0;
            string tmp="";
            string sha="";
            string uname="";
            mlog(cmd);
            for(char ch : cmd)
                {
                    if(ch==' ')
                    {
                        

                        if(flag==1)
                        {
                            uname=tmp;
                            tmp="";
                        }

                        if(flag>1 && tmp!="")                   
                        {
                            gname.push_back(tmp);
                            tmp="";
                        }
                        flag++;
                        continue;
                    }
                    if(flag==0)
                        sha=sha+ch;
                    if(flag>0)
                        tmp=tmp+ch;
                }
                cout<<cmd<<endl;
                mlog("updateInfo : "+sha+" "+uname);

                for(string grpid:gname)
                {
                    if(group.find(grpid)==group.end())
                    {

                    }
                    else
                    {   
                        vector<string> memlist=group[grpid];

                        if(find(memlist.begin(),memlist.end(),uname)==memlist.end())
                        {
                            //string msg="Only Member of Group Can Share File!!!\n";
                            
                        }
                        else
                        {
                            if(find(filesgrp[grpid][sha].begin(), filesgrp[grpid][sha].end(), uname) ==filesgrp[grpid][sha].end())
                            filesgrp[grpid][sha].insert(uname);

                            string msg="File info updated successfully now you are seeder \n";
                            mlog(msg+" "+uname);
                        
                            //======================
                            strcpy(buff,s.c_str());
                            send(subTrackerSockNum, buff, sizeof(buff), 0);
                            pushCommand(s);
                            //cout<<buff;
                            //======================

                        }
                    }


                }
                break;
            
        }
        else if(s.substr(0,10)=="stop_share")
        {
            string cmd = s.substr(11,s.size()-11);
            
            string grpid="";
            string filename="";
            string uname = username;
            
            mlog(cmd);
            int flag=0;
            for(char ch : cmd)
                {
                    if(ch==' ')
                    {   
                        flag++; 
                        continue;
                    }
                    if(flag==0)
                        grpid=grpid+ch;
                    if(flag==1)
                        filename=filename+ch;

                }

            cout<<"******"<<filename<<"---"<<grpid<<endl;
            string sha="";
            for(auto it : filesgrp)
            {
                cout<<it.first<<" ";
                if(it.first == grpid)
                {
                    for(auto st:it.second)
                    {
                        cout<<st.first<<" "<<shaname[st.first]<<endl;
                        if(shaname[st.first]==filename)
                        {
                            sha=st.first;
                        }
                        
                    }
                    
                }
            }

            if(sha!="")
               { 

                filesgrp[grpid][sha].erase(username);
                string msg="You are no longer sharing this file!!!";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);

                //======================
                s=s+" "+username;
                strcpy(buff,s.c_str());
                send(subTrackerSockNum, buff, sizeof(buff), 0);
                pushCommand(s);
                //cout<<buff;
                //======================

               }
            else
            {
                string msg="Either File of grp not found";
                strcpy(buff,msg.c_str());
                send(sock, buff, sizeof(buff), 0);
  
            }



                
         
            
        }
        else if(s.substr(0,10)=="SUBTRACKER")
        {
            cout<<"Adding SUB TRACKER ENTRY";            
            subTrackerSockNum=sock;
            username="SUBTRACKER";

            string msg="Adding SUB TRACKER ENTRY";            
            //strcpy(buff,msg.c_str());
            cout<<msg;

            int emptyFlag=0;


            //***********************************
            myfile.close();
            
            ifstream in("tracker.txt");
            
            in.seekg(0, ios::end); 
            if(in.tellg() == 0)
                emptyFlag=1;

            in.close();
            myfile.open("trackersLog.txt",ios::app);
            //*****************************************
            
            if(emptyFlag)
            {
                while(read(sock,buff,sizeof(buff)) > 0)
                {
                    string line(buff);
                    cout<<line<<endl;
                    receivedCommands(line);
                }
            }
            else
            {

                ifstream in("tracker.txt");
                string line;
                while(getline(in,line))
                {
                    cout<<line<<endl;
                    strcpy(buff,line.c_str());    
                    send(sock, buff, sizeof(buff), 0);        
                }    
                in.close();

            }


        }
        else
        {
            string msg="Please Check Your Command!!!";
            strcpy(buff,msg.c_str());
            send(sock, buff, sizeof(buff), 0);

        }
    }

    cout<<"Client Finished!!!";
    close(sock);
    
}



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

































void readS(int sock)
{
     cout<<"read S";
    char buff[1024]={0};
    while(keepRunning)
    {
    
    int t=read(sock,buff,sizeof(buff));
    cout<<"=="<<t<<endl;
    if(t<=0)
        {
            sleep(3);
            sock = getconsock(trackerInfo[0].first,trackerInfo[0].second);
            
            if(sock>0)
            {
                string line = "SUBTRACKER";
                        strcpy(buff,line.c_str());
                        send(sock, buff, sizeof(buff), 0);
            
                        //commandLog.close();
            
                        ifstream in("tracker.txt");
            
                        while(getline(in,line))
                        {
                            strcpy(buff,line.c_str());    
                            send(sock, buff, sizeof(buff), 0);        
                        }    
                        in.close();
            
            
                        //commandLog.open("tracker.txt", ios :: app);
            }

        }
        else
        {
            cout<<"++++++"<<buff<<endl;
            string s(buff);
            pushCommand(s);
            receivedCommands(s);
        }    
    }
}
void writeS(int sock)
{
    cout<<"write S";
    char buff[1024];

    string line = "SUBTRACKER";
    strcpy(buff,line.c_str());
    send(sock, buff, sizeof(buff), 0);


    //********************************************
    

    ifstream in("tracker.txt");

    while(getline(in,line))
    {
        cout<<line<<endl;
        strcpy(buff,line.c_str());    
        send(sock, buff, sizeof(buff), 0);        
    }    
    in.close();

    //********************************************


    while(keepRunning)
    {
    
    string line;
    
    getline(cin,line);

    if(line=="quit")
    {
        exit(0);
    }

    strcpy(buff,line.c_str());

    send(sock, buff, sizeof(buff), 0);
    }

    //cout<<"connection terminated";
}

void quit()
{
    while(keepRunning)
    {
    
        string line;
        
        getline(cin,line);

        if(line=="quit")
        {
            keepRunning=false;
            exit(0);
        }
    }
}

void receivedCommands(string s)
{
    char buff[1024];
    string username="test";   
    int login=0; 
    string connectport;
    bool kr=true;
    

        if(s.substr(0,11)=="create_user")
        {
            int flag=0;
            string str = s.substr(12,s.size()-12);
            string username="";
            string password="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    username=username+ch;
                if(flag==1)
                    password=password+ch;
            }
            if(user.find(username) == user.end())
            {
                mlog("SUBTRACKER:"+s);
                user.insert({username,password});
            }
        }
        else if(s.substr(0,5)=="login")
        {   

            int flag=0;
            string str = s.substr(6,s.size()-6);
            username="";
            string password="";
            string ip="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    username=username+ch;
                if(flag==1)
                    password=password+ch;
                if(flag==2)
                    ip=ip+ch;
            }
            
            if(user.find(username)!=user.end() && user[username]==password)
            {
                //marking user logged in.
                mlog("SUBTRACKER:"+s);
                userConn[username]=ip;
            }
        }
        else if(s.substr(0,6)=="logout")
        {
            
            username = s.substr(7,s.size()-7);
            mlog("SUBTRACKER:"+s);
            userConn[username]="";
        }
        else if(s.substr(0,12)=="create_group")
        {

            string str = s.substr(13,s.size()-13);


            string grpid="";
            string username="";
            int flag=0;
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    grpid=grpid+ch;
                if(flag==1)
                    username=username+ch;
                
            }

            
            if(group.find(grpid)==group.end())
            {
                group[grpid].push_back(username);
                mlog("SUBTRACKER:"+s);
            }        
        }
        else if(s.substr(0,11)=="leave_group")
        {
            string str = s.substr(12,s.size()-12);

            string grpid="";
            string username="";
            int flag=0;
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    grpid=grpid+ch;
                if(flag==1)
                    username=username+ch;
                
            }

            if(group.find(grpid)!=group.end())
            {
                vector<string> memList = group[grpid];
                auto it = find(memList.begin(),memList.end(),username);
                if(it!=memList.end())
                {
                    group[grpid].erase(it);

                    if(group[grpid].empty())
                    {
                        group.erase(grpid);
                    }
                    
                    mlog("SUBTRACKER:"+s);
                }
                
            }
        }
        else if(s.substr(0,10)=="join_group")
        {
            string str = s.substr(11,s.size()-11);


            string grpid="";
            string username="";
            int flag=0;
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    grpid=grpid+ch;
                if(flag==1)
                    username=username+ch;
                
            }


            if(group.find(grpid)!=group.end())
            {
                
                    pending[grpid].insert(username);
                    mlog("SUBTRACKER:"+s);
            }
            
               
        }
        else if(s.substr(0,14)=="accept_request")
        {
            string str = s.substr(15,s.size()-15);
            string grpid="";
            string userid="";
            string username="";
            
            int flag=0;
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    grpid=grpid+ch;
                if(flag==1)
                    userid=userid+ch;
                if(flag==2)
                    username=username+ch;
                
            }

            cout<<s<<endl;
            cout<<grpid<<" "<<userid<<" "<<username;

            pair<string, string> arg = get2arg(s.substr(15,s.size()-15));
            //string grpid=arg.first;
            //string userid=arg.second;





            if(pending.find(grpid)!=pending.end())
            {
                if(!group[grpid].empty() && group[grpid][0]==username)
                {
                    unordered_set<string> memList = pending[grpid];

                    if(memList.find(userid)!=memList.end())
                    {
                        group[grpid].push_back(userid);
                        pending[grpid].erase(userid);
                        
                        if(pending[grpid].empty())
                        {
                            if(pending.erase(grpid));
                                //cout<<"key found erasing";

                        
                        }
                        
                        mlog("SUBTRACKER:"+s);
                    }
                    cout<<"level1"<<endl;
                    
                }
                cout<<"level2"<<endl;
                
            }

            
               
        }
        else if(s.substr(0,11)=="upload_file")
        {
            int flag=0;
            string str = s.substr(12,s.size()-12);
            string filepath="";
            string grpid="";
            string sha="";
            string username="";
            for(char ch : str)
            {
                if(ch==' ')
                {
                    flag++;
                    continue;
                }
                if(flag==0)
                    filepath=filepath+ch;
                if(flag==1)
                    grpid=grpid+ch;
                if(flag==2)
                    sha=sha+ch;
                if(flag==3)
                    username=username+ch;
            }
            

            size_t found=filepath.find_last_of('/');

            if(found!=string::npos)
                filepath=filepath.substr(found+1);

            
            vector<string> memlist=group[grpid];

        
                if(shaname.find(sha)==shaname.end())
                {
                    string fname;

                    if(filepath[0]=='.' || filepath[0]=='~')
                        fname=filepath.substr(filepath.find_last_of("/\\") + 1);
                    else
                        fname=filepath;
                    
                    cout<<"File name "<<filepath<<endl;
                    shaname[sha]=fname;

                }
                
                filesgrp[grpid][sha].insert(username);
    
                mlog("SUBTRACKER:"+s);
            
        }
        else if(s.substr(0,5)=="IHAVE")
        {
            string cmd = s.substr(6,s.size()-6);
            vector<string> gname;
            int flag=0;
            string tmp="";
            string sha="";
            string uname="";
            mlog(cmd);
            for(char ch : cmd)
                {
                    if(ch==' ')
                    {
                        

                        if(flag==1)
                        {
                            uname=tmp;
                            tmp="";
                        }

                        if(flag>1 && tmp!="")                   
                        {
                            gname.push_back(tmp);
                            tmp="";
                        }
                        flag++;
                        continue;
                    }
                    if(flag==0)
                        sha=sha+ch;
                    if(flag>0)
                        tmp=tmp+ch;
                }
                cout<<cmd<<endl;
                mlog("updateInfo : "+sha+" "+uname);

                for(string grpid:gname)
                {
                    if(group.find(grpid)==group.end())
                    {

                    }
                    else
                    {   
                        vector<string> memlist=group[grpid];

                        if(find(memlist.begin(),memlist.end(),uname)==memlist.end())
                        {
                            //string msg="Only Member of Group Can Share File!!!\n";
                            
                        }
                        else
                        {
                            if(find(filesgrp[grpid][sha].begin(), filesgrp[grpid][sha].end(), uname) ==filesgrp[grpid][sha].end())
                            filesgrp[grpid][sha].insert(uname);

                            string msg="File info updated successfully now you are seeder \n";
                            mlog(msg+" "+uname);
                        }
                    }


                }
            
        }
        else if(s.substr(0,10)=="stop_share")
        {
            string cmd = s.substr(11,s.size()-11);
            
            string grpid="";
            string filename="";
            string uname = "";
            
            mlog(cmd);
            int flag=0;
            for(char ch : cmd)
                {
                    if(ch==' ')
                    {   
                        flag++; 
                        continue;
                    }
                    if(flag==0)
                        grpid=grpid+ch;
                    if(flag==1)
                        filename=filename+ch;
                    if(flag==2)
                        uname=uname+ch;
                }

            
            string sha="";
            for(auto it : filesgrp)
            {
                cout<<it.first<<" ";
                if(it.first == grpid)
                {
                    for(auto st:it.second)
                    {
                        cout<<st.first<<" "<<shaname[st.first]<<endl;
                        if(shaname[st.first]==filename)
                        {
                            sha=st.first;
                        }
                        
                    }
                    
                }
            }

            if(sha!="")
               { 

                filesgrp[grpid][sha].erase(username);
                string msg="You are no longer sharing this file!!!";
                mlog(msg);
                //======================
                
               }    
            
        }
    
    
}







void trackerSync()
{
    cout<<trackerInfo[0].first<<" "<<trackerInfo[0].second<<endl;
    int connfd = getconsock(trackerInfo[0].first,trackerInfo[0].second);

    cout<<"======================================="<<endl;
    cout<<"===========Connected WIth Master======="<<endl;
    cout<<"======================================="<<endl;
    if(connfd>0)
        masterTrackerSockNum=connfd;

    thread r(readS,connfd);
    thread w(writeS,connfd);
    r.join();
    w.join();



}



int main(int argc, char *argv[])
{

    commandLog.open("tracker.txt",ios::trunc);
    commandLog.close();

    myfile.open("trackersLog.txt",ios::trunc);
    myfile<<"Program startted";

    if(argc != 3)
    {
        cout<<"Run program with valid arguments";
        exit(0);
    }
    else
    {
        cout<<"Hello";
    }


    vector<string> cmd(argv,argv+argc);
    

    

    string trackerInfoFile(cmd[1]);
    string trackerNumber(cmd[2]);
    string ipport;

    cout<<trackerInfoFile<<" "<<trackerNumber<<endl;
    
    ifstream in(trackerInfoFile);
    

    

    int tnum=stoi(trackerNumber);
 
    in >> ipport;
    trackerInfo.push_back(separateIP(ipport));
    in >> ipport;
    trackerInfo.push_back(separateIP(ipport));

    in.close();
    

    
    //mlog("This Is Tracker Number :"+trackerNumber + "IP:PORT -" + ipport);
    

    
    

    PORT=stoi(trackerInfo[tnum].second);
    HOST=trackerInfo[tnum].first;
    cout<<PORT<<" "<<HOST<<endl;

    sem_init(&m,0,1);
    sem_init(&t,0,1);
    sem_init(&filelock,0,1);
    
    int sockfd, connfd, len;
    struct sockaddr_in servaddr,cli;
    
    
    // socket create and verification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");

        exit(0);
    }
    else
        printf("Socket successfully created..\n");
    
    mlog("createion successful!!!!");
    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(&HOST[0]);
    servaddr.sin_port = htons(PORT);
  
    int enable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &enable, sizeof(int)) < 0);
        
    // Binding newly created socket to given IP and verification
    if ((bind(sockfd, (sockaddr* )&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed...\n");
        exit(0);
    }
    else
        printf("Socket successfully binded..\n");
  
    

    
    if ((listen(sockfd, 50)) != 0) {
        printf("Listen failed...\n");
        exit(0);
    }
    else
        printf("Server listening..\n");
    
    socklen_t lenn = sizeof(cli);

    
    
    
    if(tnum==1)
    {

        thread master(trackerSync);
        master.detach();
    }
    else
    {
        thread qt(quit);
        qt.detach();
    }
    

    
    // Accept the data packet from client and verification
    while(keepRunning)
    {
        connfd = accept(sockfd, (sockaddr*)&cli , &lenn);
        cout<<endl<<"new conn request!!!!!!!"<<connfd<<endl;
        if (connfd < 0) {
            printf("server acccept failed...\n");
        }
        else
        {
            printf("server acccept the client...\n");
            cout<<"IP Connected : " <<inet_ntoa(cli.sin_addr)<<endl; // conver int address to ip format
            cout<<"Port Connected : "<<cli.sin_port<<endl;
            

            mlog("connected with port "+to_string(cli.sin_port));

            thread t1(handleClient,connfd,cli);
            t1.detach();
            
            
        }
        

        //cout<<"Client request completed!!!\n";
        
    
    }
    close(sockfd);
    myfile.close();
    // After chatting close the socket
    
}