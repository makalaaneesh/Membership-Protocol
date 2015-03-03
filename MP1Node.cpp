/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *              Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
 #include <string.h>
char enter;
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */
int maxtimegap=0;
int maxtgid=0;

void MP1Node::printlist(){
    cout<<"\n"<<memberNode->addr.getAddress()<<" time lags\n";
    for(int c=0;c<memberNode->memberList.size();c++){
        cout<<"id is"<<memberNode->memberList[c].getid()<<"\t";
        cout<<"heartbeat is "<<memberNode->memberList[c].getheartbeat()<<"\t";
        cout<<"timestamp is "<<memberNode->memberList[c].gettimestamp()<<"\t";
        long time1=par->getcurrtime();
        long time2=memberNode->memberList[c].gettimestamp();
        cout<<"time lag is "<<time1-time2;
        cout<<endl;
        if((time1-time2)>maxtimegap ){
            // cin>>enter;
            maxtimegap=time1-time2;
            maxtgid=memberNode->memberList[c].getid();
        }
        
        // cout<<"lag for "<<to_string(memberNode->memberList[c].getid())<<" is "<<to_string(par->getcurrtime-memberNode->memberList[c].gettimestamp())<<endl;
    }
    // cin>>enter;
    cout<<"time gap is "<<maxtimegap<<"\twith id "<<maxtgid<<endl;
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *              This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *              All initializations routines for a member.
 *              Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    MemberListEntry *y= new MemberListEntry(id,port,memberNode->heartbeat,par->getcurrtime());
    memberNode->memberList.push_back(*(y));
    delete y;
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
    // cout<<"address is"<<memberNode->addr;
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    cout<<"introducing self to group yo\n";
    cout<<"address is"<<(memberNode->addr.getAddress());
   
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group

#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
        
        

#endif
        // if((memberNode->addr.getAddress()=="1:0")&&(memberNode->memberList.size()==0)){

        //     int idnn=1;
        //     short portss=(short)0;
        //     long heartbeatss=(long)0;
        //     MemberListEntry *y= new MemberListEntry(idnn,portss,heartbeatss,par->getcurrtime());
        //     int enter;

        //     // cout<<"\n"<<to_string(y->getid())<<to_string(y->getport());
        //     // cout<<"\n"<<to_string(y->getheartbeat())<<to_string(y->gettimestamp());
        //     // cout<<"\n size of membership list is "<<memberNode->memberList.size();
        //     // // cin>>enter;

        //     memberNode->memberList.push_back(*(y));
        //     delete y;
        //     log->logNodeAdd(&memberNode->addr, &memberNode->addr);
        // }

        memberNode->inGroup = true;
    }
    else {

        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;

        // string datas="";
        // datas+=(memberNode->addr.getAddress())+to_string(memberNode->heartbeat);
        // cout<<"data is"<<datas;
        // cout<<" addr is "<<memberNode->addr.addr;

        char* data=new char[msgsize];
        strcat(data,"j");
        strcat(data,(memberNode->addr.getAddress()).c_str());
        strcat(data,"|");
        strcat(data,to_string(memberNode->heartbeat).c_str());
        cout<<"\ndata is---"<<data;


        // void * v1=memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        // void * v2=memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        // // cout <<"msg coming up\n"<<(char *)v1<<(char *)v2<<*(&memberNode->heartbeat)<<endl;
        // char *vv1=static_cast<char*>(v1);
        // char *vv2=static_cast<char*>(v2);
        // cout<<endl<<"v1"<<*(vv1)<<endl<<"v2"<<*(vv2)<<endl;
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        // emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        emulNet->ENsend(&memberNode->addr, joinaddr, data, msgsize);
        // cout<<"the message is\n"<<(char*)msg;
        free(msg);
        //adding self to the membership list
        log->logNodeAdd(&memberNode->addr, &memberNode->addr);
        memberNode->inGroup = true;

         
    }

    return 1;

}
int counter=0;

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
     
    cout<<runcounter<<"\t"<<memberNode->heartbeat<<endl;
    cout<<"\nTHE END\n";
    if(memberNode->inited){
        memberNode->memberList.clear();
        memberNode->inited=false;
        memberNode->heartbeat=0;
        memberNode->inGroup =false;

    }
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *              Check your messages in queue and perform membership protocol duties
 */
 bool entered=false;
 
void MP1Node::nodeLoop() {
    // cout<<"this is the nodeloop yo";
    runcounter++;
    if (memberNode->bFailed) {
       
        return;

    }
    printlist();
    //cin>>enter;
    // cout<<"this is nodeloop yo";

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    // printlist();
    //cin>>enter;
    
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

string parsereply(vector<MemberListEntry> &memberList){
    string x="";
    for(int i=0;i<memberList.size();i++){
        x=x+to_string(memberList[i].getid())+"|"+to_string(memberList[i].getport())+"|"+to_string(memberList[i].getheartbeat())+"|"+to_string(memberList[i].gettimestamp());
        x=x+"#";
    }
    
    return x;
}

void MP1Node::updatemembershiplist(Address* addr,char *data, vector<MemberListEntry> &memberList){
        char *id=new char[9];
        char *port=new char[4];
        char *heartbeat=new char[19];
        char *timestamp=new char [4];
        int i=0,cnt=0;
        while(data[i]!='\0'){
            // cout<<i;
            // i=0;
            cnt=0;
            for(i;data[i]!='|';i++){
            id[cnt++]=data[i];
            }
            id[cnt]='\0';
            cnt=0;
            for(i=i+1;data[i]!='|';i++){
            port[cnt++]=data[i];
            }
            port[cnt]='\0';
            cnt=0;
            for(i=i+1;data[i]!='|';i++){
            heartbeat[cnt++]=data[i];
            }
            heartbeat[cnt]='\0';
            cnt=0;
            for(i=i+1;data[i]!='#';i++){
            timestamp[cnt++]=data[i];
            }
            timestamp[cnt]='\0';
            i++;
           
            int idn=atoi(id);
            short ports=(short)atoi(port);
            long hbl=strtol(heartbeat,NULL,10);
            // cout<<hbl;
            //cin>>enter;
            long ts=strtol(timestamp,NULL,10);

            bool present=false;
            int c;
            for(c=0;c<memberList.size();c++){
                if(memberList[c].getid()==idn){
                    present=true;
                    break;
                }
            }
            if(!present){
                ts=par->getcurrtime();
                MemberListEntry *x= new MemberListEntry(idn,ports,hbl,ts);
                memberList.push_back(*x);
                delete x;
                string a1=to_string(idn)+":"+to_string(ports);
                // cout<<"\nsender address is:"<<a1;
                Address * sender=new Address(a1);
                log->logNodeAdd(&memberNode->addr, sender);
            }
            else if(present){

                // memberList[c].settimestamp(par->getcurrtime());
                if(hbl>memberList[c].getheartbeat()){
                    memberList[c].setheartbeat(hbl);
                }
                if(ts>memberList[c].gettimestamp()){
                    memberList[c].settimestamp(ts);
                }


                // cout<<endl<<hbl<<endl<<idn<<endl;
               // cin>>enter;
            }

        }
}
Address* getAddressFromMemberListEntry(MemberListEntry m){
        string a1=to_string(m.getid())+":"+to_string(m.getport());
        Address * sender=new Address(a1);
        return sender;
}
/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */
     Member *mn=static_cast<Member*>(env);
     
     cout<<"\n************************\n";
     
     if(mn->addr.getAddress()=="1:0"){
        if(data[0]=='j'){
            int enter;
            cout<<"heartbeat is \t"<<mn->heartbeat;
            // cin>>enter; 
            cout<<"address for the checking one is "<<mn->addr.getAddress();
            cout<<"got your message bro\t"<<data;
            char *id=new char[9];
            char *port=new char[4];
            char *heartbeat=new char[19];
            int i=1,cnt=0;
            for(i;data[i]!=':';i++){
            id[cnt++]=data[i];
            }
            id[cnt]='\0';
            cnt=0;
            for(i=i+1;data[i]!='|';i++){
            port[cnt++]=data[i];
            }
            port[cnt]='\0';
            cnt=0;
            for(i=i+1;data[i]!='\0';i++){
            heartbeat[cnt++]=data[i];
            }
            heartbeat[cnt]='\0';

            int idn=atoi(id);
            short ports=(short)atoi(port);
            long hbl=strtol(heartbeat,NULL,10);



            //creating a memberlist and adding it to the memberlist vector
            MemberListEntry *x= new MemberListEntry(idn,ports,hbl,par->getcurrtime());
            memberNode->memberList.push_back(*x);
            delete x;
            cout<<"\njust added a member. size of list is:"<<memberNode->memberList.size();

            cout<<"\nid is "<<idn;
            cout<<"\nport is "<<ports;
            cout<<"\nheartbeat is "<<hbl;
            string a1=to_string(idn)+":"+to_string(ports);
            cout<<"\nsender address is:"<<a1;
            Address * sender=new Address(a1);
            log->logNodeAdd(&memberNode->addr, sender);

            //joinrep message
            string reply;
            reply=parsereply(memberNode->memberList);
            // cout<<reply;
            char *r =new char[reply.length()+1];
            strcpy(r,reply.c_str());
            // cout<<r;

            cout<<"\n-------------------"<<r<<"----------------------\n";
            //cin>>enter;
            //sending the reply
            emulNet->ENsend(&memberNode->addr, sender, r, reply.length()+1);
        }
        else if (data[0]=='r'){
            int i=1;
            int cnt=0;
            char * id=new char[3];
            while(data[i]!='\0'){
                id[cnt++]=data[i++];
            }
            id[cnt]='\0';
            int idn=atoi(id);
            int index=-1;
            for(int i=0;i<memberNode->memberList.size();i++){
                if(memberNode->memberList[i].getid()==idn){
                    index=i;
                    break;
                }
            }
            if(index>0){
                log->logNodeRemove(&memberNode->addr,getAddressFromMemberListEntry(memberNode->memberList[index]));
                memberNode->memberList.erase(memberNode->   memberList.begin()+index);
            }
        }
        else{
            updatemembershiplist(&memberNode->addr,data,memberNode->memberList);
        }
    }
    else{
        if (data[0]=='r'){
            int i=1;
            int cnt=0;
            char * id=new char[3];
            while(data[i]!='\0'){
                id[cnt++]=data[i++];
            }
            id[cnt]='\0';
            int idn=atoi(id);
            int index=-1;
            for(int i=0;i<memberNode->memberList.size();i++){
                if(memberNode->memberList[i].getid()==idn){
                    index=i;
                    break;
                }
            }
            if(index>0){
                log->logNodeRemove(&memberNode->addr,getAddressFromMemberListEntry(memberNode->memberList[index]));
                memberNode->memberList.erase(memberNode->memberList.begin()+index);
            }
        }
        else{
            cout<<"\n This is "<<mn->addr.getAddress()<<". Updating membership list";
            // cout<<"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$This is the a node other than the introducer. THis means that this node has got a message. ";
            // cout<<"\n$$$$$$$$$$$$$$$$$$$$$$$$$$$$$THe address is "<<mn->addr.getAddress();
            updatemembershiplist(&memberNode->addr,data,memberNode->memberList);
            // cout<<"\n$$$$$$$$type is :"<<type(&memberNode->addr);
        }
    }
     cout<<"\n************************\n";
    

}



int getpos(vector<MemberListEntry> &memberList,Address *addr){
    string myaddr=addr->getAddress();
    char * ma=new char[8];
    char * id_s=new char[3];
    strcpy(ma,myaddr.c_str());
    int cnt=0;
    for (int i=0;ma[i]!=':';i++){
        id_s[cnt++]=ma[i];
    }
    id_s[cnt]='\0';
    int id=atoi(id_s);
    int c=0;
    for(c=0;c<memberList.size();c++){
        if (memberList[c].getid()==id){
            break;
        }
    }
    return c;

}



void MP1Node::deleteandpropagate(vector<MemberListEntry> &memberList){
    int GRANT_TIME=3;
    int index=-1;
    vector<int> i_list;

    string send="r";
    for(int c=0;c<memberList.size();c++){
        if (par->getcurrtime()-memberList[c].gettimestamp()>GRANT_TIME){
            index=c;
            i_list.push_back(index);
            send=send+to_string(memberList[c].getid());
            char* sendc=new char[4];
            strcpy(sendc,send.c_str());
            for(int d=0;d<memberNode->memberList.size();d++){
                Address * tosend=getAddressFromMemberListEntry(memberNode->memberList[d]);
                emulNet->ENsend(&memberNode->addr, tosend, sendc, send.length()+1);
            }

        }
    }
    for(int c=0;c<i_list.size();c++){

        log->logNodeRemove(&memberNode->addr,getAddressFromMemberListEntry(memberList[i_list[c]]));
        memberList.erase(memberList.begin()+i_list[c]);

    }

}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *              the nodes
 *              Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    /*
     * Your code goes here
     */

     deleteandpropagate(memberNode->memberList);
     if(memberNode->memberList.size()==0){
        return;
     }
     char enter;
    memberNode->heartbeat++;
    int poss=0;
    poss=getpos(memberNode->memberList,&memberNode->addr);

    // cout<<poss<<endl;
    // cout<<memberNode->addr.getAddress()<<endl;
    // cout<<memberNode->memberList[poss].getid();
    // cin>>enter;
    memberNode->memberList[poss].setheartbeat(memberNode->heartbeat);
    memberNode->memberList[poss].settimestamp(par->getcurrtime());
    

    
    // cin>>enter;
    string reply;
    reply=parsereply(memberNode->memberList);
    char *r =new char[reply.length()+1];
    strcpy(r,reply.c_str());
    cout<<r;
    //cin>>enter;

    for(int c=0;c<memberNode->memberList.size();c++){
        Address * tosend=getAddressFromMemberListEntry(memberNode->memberList[c]);
        emulNet->ENsend(&memberNode->addr, tosend, r, reply.length()+1);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}