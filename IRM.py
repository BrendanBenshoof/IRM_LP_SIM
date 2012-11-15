##Copyright (C) <2012> <Brendan Benshoof and Andrew Rosen>

##Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

##The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

##THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


##networkX, and chord demo

import networkx as nx #makes graphs easy, and makes pretty pictures
import random
import numpy
import random as plt
## renders the prety pictures

from math import log
##we need to log function to kow how many skips to make

import random
##for random numbers

##so we can know the size of our hash

#GODLIKE Time manager





class Simulator:
    def __init__(self):
        self.time = 0
        self.Topo = ""
        self.links = []
        self.nodes = []
        self.files = []
        self.link_min = 0.05
        self.link_max = 0.1
        self.alpha = 0.5
        self.beta = 1.5
        self.file_gen = {}
        self.IRM_Nodes = []
        self.packets = 0
        self.total_hops = 0
        self.special_hops=0
        self.m = 16
        self.event_schedual = []
        self.IRM_request_frequency = 0.5
        self.IRM_forward_frequency = 1.0
        self.replica_timeout_min = 25.0
        self.replica_timeout_max = 100.0
        self.replica_timeout = {}
        self.replica_total=0
        self.query_replicas=0
        self.forward_replicas=0
        self.hit_rate_sucess=0.0
        self.hit_rate_total=0.0
        self.poll_messages=0
        self.replica_reponses = 0
        self.total_reponses = 0
        #THINGS TO MESS WITH
        self.replicas_can_answer_poll = False
        self.greedy_polling = False
        self.conditional_polling = False
        self.IRM_IS_ON = True
        self.replica_timeout_mean = 50.0
        #THINGS TO MESS WITH ENDS HERE
        
        
        self.intercepted_polls = 0

        self.progress = 0.0

    def run(self):
        maxtime = 100.0

            

        delta_t = 0;
        lasttime_was_inf = False
        finish_stage = 0
        while(self.time <maxtime):
            self.time+=delta_t
            times = []
            for r in self.IRM_Nodes:
                r.process_node()
            for n in self.nodes:
                n.sort_mail()
            for l in self.links:
                times.append(l.tick(delta_t))
            delta_t = min(times)
            if(len(self.event_schedual)>0):
                if(self.event_schedual[0][0]-self.time<=delta_t):
                    delta_t = self.event_schedual[0][0]-self.time
                    f = self.event_schedual[0][1]
                    try:
                        args = self.event_schedual[0][2]
                        f(args)
                    except(IndexError):
                        f()
                    self.event_schedual.remove(self.event_schedual[0])
            if delta_t==float("inf") and finish_stage==3:
                self.time = maxtime+1
                print "Done"
            elif delta_t==float("inf"):
                delta_t=0
                lasttime_was_inf = True
                finish_stage+=1
            else:
                lasttime_was_inf = False
                finish_stage=0
            for f in self.files:
                if(self.time+delta_t>self.file_gen[f]+self.replica_timeout[f]):
                    delta_t = self.file_gen[f]+self.replica_timeout[f]-self.time
                    self.file_gen[f] = sim.file_gen[f]+self.replica_timeout[f]
                    

            #print "delta t =",delta_t

    def node_by_hash(self,hashval):
        for i in self.nodes:
            if i.hashid == hashval:
                return i


sim = Simulator()

class Chord_Topology:
    ##this object organizes all of our other classes
    ##not quite a god object
    ## it builds a graph for the chord topology
    ## stores a DHT_node on every node
    ## stores a Link on every edge and sets up the pointers
    ## stores IRM_nodes on every node and sets up pointers
    def __init__(self,size):
        self.size = size
        self.G = self.make_chord(size)
        self.setup_links()


    def make_chord(self,n): ##setup the chord netowrk
        hashes = []
        for i in range(0,n):
            hashes.append(random.randint(0,2**sim.m)) ##give everything hashes
        hashes = sorted(hashes)[::-1]

        G = nx.DiGraph()
        G.add_nodes_from(hashes)

        loc = 0
        #print hashes
        predecessors = []
        for i in hashes:
            #print "for "+str(i)+":"
            G.node[i]['inbox'] = []
            N = DHT_node(i,G.node[i]['inbox'])
            sim.nodes.append(N)
            for j in range(0,sim.m):
                scale = 2**j
                best = -1
                for k in hashes:
                    if(k!=i):
                        if(k>=(scale+i)%(2**sim.m)):
                            best = k
                if(best==-1):
                    best = hashes[-1]
                #print(best)
                G.add_edge(i,best)
                N.finger.append(G.edge[i][best])
                if(j==0):
                    predecessors.append((best,N))
            #print ""
        for i in predecessors:
            for j in sim.nodes:
                if(i[0]==j.hashid):
                    j.prev = i[1].hashid
            
                
            loc+=1
        return G

    def setup_links(self):
        #setup all the link objects on the edges
        counter = 0
        for e0 in self.G.edge:
            self.G.node[e0]["Links"] = {}
            for e1 in self.G.edge[e0]:
                L = Link(sim.link_min+random.random()*(sim.link_max-sim.link_min),.99,self.G.node[e1])
                sim.links.append(L)
                self.G.edge[e0][e1]['Link'] = L
                self.G.node[e0]["Links"][e1] = L
                L.inbox = self.G.node[e1]['inbox']
            sim.node_by_hash(e0).setup_links(self.G)
            counter+=1

    def remove_node(self,id):
        try:
            to_update = self.G.edge[id]
        except(KeyError):
            return
        while(True):#remove multiple entries?
            try:
                self.G.remove_node(id)
                sim.Topo.remove_node(id)
            except(nx.exception.NetworkXError):
                break
            sim.nodes.remove(sim.node_by_hash(id))
        self.setup_links()
    
    def add_node(self): ##setup the chord netowrk
        new = random.randint(0,2**sim.m)
        hashes = [new]
        for i in sim.nodes:
             hashes.append(i.hashid)##give everything hashes
        hashes = sorted(hashes)[::-1]

        G = self.G
        G.add_node(new)
        #print hashes
        predecessors = []
        #print hashes
        for i in hashes:
            #print "for "+str(i)+":"
            if(i==new):
                G.node[i]['inbox'] = []
                N = DHT_node(i,G.node[i]['inbox'])
                sim.nodes.append(N)
            else:
                N = sim.node_by_hash(i)
            N.finger = []
            for j in range(0,sim.m):
                scale = 2**j
                best = -1
                for k in hashes:
                    if(k!=i):
                        if(k>=(scale+i)%(2**sim.m)):
                            best = k
                if(best==-1):
                    best = hashes[-1]
                #print(best)
                G.add_edge(i,best)
                
                N.finger.append(G.edge[i][best])
                if(j==0):
                    predecessors.append((best,N))
            #print ""
        for i in predecessors:
            for j in sim.nodes:
                if(i[0]==j.hashid):
                    j.prev = i[1].hashid
        self.setup_links()



    def render(self):
        self.G.node = sorted(self.G.node)
        nx.draw_circular(self.G)
        plt.show()


class Message:
    def __init__(self,kind,origin,dest,size):
        self.delay = 0.0
        self.kind = kind
        self.dest = dest
        self.origin = origin
        self.size = size
        self.C = {} ##message contents, just put stuff in here under headers
        ##make sure to standardize fieldnames and data structure with kinds of messages




class Link:
    def __init__(self, delay, reliability, Dest_inbox):
        self.delay = delay
        self.previous = -1
        self.reliability = reliability
        self.outbox = []
        self.intransit = []
        self.inbox = Dest_inbox ##modifies destination object

    def send_msg(self,msg):
        if(msg.kind=="GET" or msg.kind=="RESP"):
            sim.total_hops+=1
        elif(msg.kind=="POLL" or msg.kind=="POLLRESP"):
            sim.special_hops+=1
        msg.delay = self.delay
        while random.random() > self.reliability: ##roll for critical message send failure
            #print "a message sending failure happened!"
            msg.delay += self.delay*2 ##return failure to sender and resend
        self.outbox.append(msg)

    def tick(self,delta_t):
        #print self.outbox,self.intransit
        self.intransit+=self.outbox[:]
        self.outbox = []
        #be slightly creeped out that this works
        min = float("inf")
        for msg in self.intransit:
            msg.delay-=delta_t
            if msg.delay<=0:
                #print msg,"has arrived to be processed"
                self.inbox.append(msg)
                self.intransit.remove(msg)
            elif msg.delay < min:
                #print msg, "is still in libo"
                min = msg.delay
        
        if(len(self.intransit)>0):
            return min
        else:
            return float("inf")

class DHT_node:
    def __init__(self,myid,inbox):
        self.hashid = myid
        self.sucessors = []
        self.finger = []
        self.prev = -1

        self.msghandlers = {"GET":handle_get(),"STORE":handle_store()}

        self.files = {} ## a place to store our files
        self.inbox = inbox
        self.help_me_box = []##put all unhandleable messages here for IRM to deal with

    def can_handle(self,msg):
        for handler in self.msghandlers.values():
            if(handler.test(msg.kind)):
                return True
        return False


    def setup_links(self,graph):
        peerlist = graph.edge[self.hashid].keys()
        for i in range(0,len(peerlist)):
            self.sucessors.append(peerlist[i])


    def get_sucessor(self):
        best = self.sucessors[0]
        bestscore = 2**sim.m
        for i in self.sucessors:
            V = i - self.hashid
            if(V<0):
                V = -1*V
            if(V<bestscore):
                best = i
                bestscore = V
        return best

    def best_route(self,dest):#returns the inbox of the closest destination to the desired id
        #returns Null if the best distination is ourselves
        #print self.prev,self.hashid, destb
        if(dest>self.prev and dest<=self.hashid):
            return None
        elif (self.prev>=self.hashid and (dest<=self.hashid or dest>self.prev)):
                return None
        V= dest-self.hashid
        if(V==0):
            return None
        elif(V<0):
            V = 2**sim.m-self.hashid+dest-1
            skips = int(log(V,2))-1
        else:
            skips = int(log(V,2))
        #print V,skips

        
        return self.finger[skips]['Link']


    def handle_message(self,msg):#takes a message and handles it
        self.msghandlers[msg.kind].handle(self,msg)


    def sort_mail(self):#goes though my mail and decides what to do with it
        for mail in self.inbox:
            nexthop = self.best_route(mail.dest)
            if(nexthop is None):#this is addressed to me
                #print(self.hashid,"is dealing with mail destined to",mail.dest)
                if self.can_handle(mail):
                    self.inbox.remove(mail)
                    self.handle_message(mail)
                else:
                    self.help_me_box.append(mail)
                    self.inbox.remove(mail)
            else:#it goes to somebody else, best of luck!
                #print(self.hashid,"is forwarding mail towards",mail.dest)
                self.inbox.remove(mail)
                nexthop.send_msg(mail)



class IRM_Node:
    def __init__(self,my_node):
        self.host = my_node
        self.query_history = {}#key: destination value: tuple (#number of times,first request time)
        self.forward_history = {} #key destination,  val: (#number of forwards, first forward)
        self.replications = {}
        self.polling=[]

     
    def process_node(self):
        #go though my host's inbox for things of interest
        #update my pile of statitics
        #check if I need to take actions
        #example: need to make a replication based on statistics, need to update a replication (timer), need to reply to a replica
     
        
        for x in self.host.inbox:
            if(x.kind == "POLL"):
                if(self.host.best_route(x.dest)==None):
                    msg = Message("POLLRESP",x.dest,x.origin,0)
                    msg.C['time'] = sim.file_gen[x.dest]
                    msg.C['file'] = "meh, implement later"
                    self.host.inbox.append(msg)
                    del x
                elif(sim.replicas_can_answer_poll and x.dest in self.replications.keys()):
                    t = self.replications[x.dest][0]
                    if(t>float(x.C['time'])):
                        msg = Message("POLLRESP",x.dest,x.origin,0)
                        msg.C['time'] = str(t)
                        msg.C['file'] = "meh, implement later"
                        self.host.inbox.append(msg)
                        sim.intercepted_polls+=1
                        x.dest = self.host.hashid
                        x.kind = "ohgodkillme"
                        del x    



            elif(x.kind == "GET"):
                if(sim.conditional_polling and x.dest in self.replications.keys()):
                    if(x.dest in self.forward_history.keys()):
                        if(self.replications[x.dest][0]+sim.replica_timeout[x.dest]<sim.time and self.replications[x.dest][0]+self.forward_history[x.dest][1]/self.forward_history[x.dest][0]<sim.time):
                          self.remakeReplica(x.dest)
                    elif(x.dest in self.query_history.keys()): #change back if sudden strike of suck
                        if(self.replications[x.dest][0]+sim.replica_timeout[x.dest]<sim.time and self.replications[x.dest][0]+self.query_history[x.dest][1]/self.query_history[x.dest][0]<sim.time):
                          self.remakeReplica(x.dest)
                if(x.origin == self.host.hashid):
                    #update query stats
                    try:
                        entry  = self.query_history[x.dest]
                        newentry = (entry[0]+1,entry[1])
                        self.query_history[x.dest] = newentry
                        if(newentry[0]/(sim.time-newentry[1]+0.01)>sim.IRM_request_frequency):
                            if(self.makeReplica(x.dest)):
                                sim.query_replicas+=1.0
                    except(KeyError):
                        self.query_history[x.dest] = (1,sim.time)
                else:
                    try: 
                        entry  = self.forward_history[x.dest]
                        newentry = (entry[0]+1,entry[1])
                        self.forward_history[x.dest] = newentry
                        #print newentry[0]/(sim.time-newentry[1])
                        if(newentry[0]/(sim.time-newentry[1])>sim.IRM_forward_frequency):
                            if(self.makeReplica(x.dest)):
                                sim.forward_replicas+=1.0
                    except(KeyError):
                        self.forward_history[x.dest] = (1,sim.time)
                if(x.dest in self.replications.keys() and not x.dest in self.polling):
                    if(not self.replications[x.dest][0]==-1.0):
                        if(sim.file_gen[x.dest]>self.replications[x.dest][0]):
                            sim.hit_rate_sucess-=1.0
                    	self.host.handle_message(x)
                        sim.replica_reponses+=1
            elif(x.kind =="POLLRESP" and sim.greedy_polling and not x.dest == self.host.hashid):
            	if(x.origin in self.replications.keys()):
                    t = float(x.C['time'])
                    if(t>self.replications[x.origin][0]):
                        self.replications[x.origin] = (sim.time,x.C['file'],x.origin,self.replications[x.origin][3]/sim.beta)
                        self.host.files[x.origin] = x.C['file']
                    else:
                        self.replications[x.origin] = (self.replications[x.origin][0],self.replications[x.origin][1],self.replications[x.origin][2],self.replications[x.origin][3]+sim.alpha)
            elif(x.kind =="POLLRESP" and x.dest == self.host.hashid):
                if(x.origin in self.replications.keys()):
                    t = float(x.C['time'])
                    if(x.origin in self.polling):
                        self.polling.remove(x.origin)
                    if(t>self.replications[x.origin][0]):
                       
                        self.replications[x.origin] = (sim.time,x.C['file'],x.origin,min([self.replications[x.origin][3]/sim.beta,sim.replica_timeout_min]))
                        self.host.files[x.origin] = x.C['file']
                    else:
                        self.replications[x.origin] = (sim.time,x.C['file'],x.origin,max([x.origin,self.replications[x.origin][3]+sim.alpha,sim.replica_timeout_max]))#fiddle with me!

        for r in self.replications.values():
            if(r[2] in self.forward_history.keys()):
                if(self.forward_history[r[2]][0]/self.forward_history[r[2]][1]<sim.IRM_forward_frequency):
                    if(not r[2] in self.query_history.keys()):
                        try:
                            del self.host.files[r[2]]
                            del self.replications[r[2]]
                            sim.forward_replicas-=1
                        except(KeyError):
                            pass
                    elif(self.query_history[r[2]][0]/self.query_history[r[2]][1]<sim.IRM_request_frequency):
                        try:
                            del self.host.files[r[2]]
                            del self.replications[r[2]]
                            sim.forward_replicas-=1
                        except(KeyError):
                            pass
                elif(not sim.conditional_polling and r[0]+sim.replica_timeout[r[2]]<sim.time and r[0]+r[0]+self.forward_history[r[2]][1]/self.forward_history[r[2]][0]<sim.time):
        		    self.remakeReplica(r[2])
            elif(r[2] in self.query_history.keys()):
                if(self.query_history[r[2]][0]/self.query_history[r[2]][1]<sim.IRM_request_frequency):
                    if(not r[2] in self.forward_history.keys()):
                        try:
                            del self.host.files[r[2]]
                            del self.replications[r[2]]
                            sim.query_replicas-=1
                        except(KeyError):
                            pass
                    elif(self.forward_history[r[2]][0]/self.forward_history[r[2]][1]<sim.IRM_forward_frequency):
                        try:
                            del self.host.files[r[2]]
                            del self.replications[r[2]]
                            sim.query_replicas-=1
                        except(KeyError):
                            pass   
                elif(not sim.conditional_polling and r[0]+sim.replica_timeout[r[2]]<sim.time and r[0]+self.query_history[r[2]][1]/self.query_history[r[2]][0]<sim.time):
                    self.remakeReplica(r[2])         
    def makeReplica(self, filename):

        filename = int(filename)
        if(not filename in self.host.files.keys()):
            if(not filename in self.replications.keys()):
                #print "making a replica of "+str(filename)+" at "+str(self.host.hashid)
                sim.replica_total+=1
                self.replications[filename] = (-1.0,"",filename,sim.replica_timeout_min)
                sim.poll_messages+=1
                msg = Message("POLL",self.host.hashid,filename,0)
                msg.C['time']=self.replications[filename][0]
                self.host.inbox.append(msg)
                self.polling.append(filename)
                return True
        return False

    def remakeReplica(self, filename):
        if(not filename in self.polling):
            self.polling.append(filename)
            sim.poll_messages+=1
            filename = int(filename)
            msg = Message("POLL",self.host.hashid,filename,0)
            msg.C['time']=self.replications[filename][0]
            self.host.inbox.append(msg)
            return True
        return False



class msghandler:
    def __init__(self,mykind):
        self.kinds = mykind

    def handle(self,origin,msg):
        print "you forgot to override this..."
        return true

    def test(self,test_kind):
        if test_kind in self.kinds:
            return True
        else:
            return False

class handle_get(msghandler):
    
    def __init__(self):
        self.kinds = ["GET"]

    def handle(self,origin,msg):
        if msg.dest in origin.files.keys():
            myfile = origin.files[msg.dest]
        else:
            myfile = "404 FILE NOT FOUND"
        newmsg = Message("RESP",msg.dest,msg.origin,msg.size)
        #print "got the file headed back"
        newmsg.C["file"] = myfile
        origin.inbox.append(newmsg)
        #print origin.inbox
        sim.packets+=1
        sim.hit_rate_total+=1
        sim.hit_rate_sucess+=1
        sim.total_reponses+=1

class handle_store(msghandler):
    
    def __init__(self):
        self.kinds = ["STORE"]

    def handle(self,origin,msg):
        origin.files[msg.dest] = msg.C["file"]


def send_request():
    dest = random.choice(sim.files)
    origin = random.choice(sim.nodes)
    kind = "GET"
    msg = Message(kind,origin.hashid,dest,0)
    sim.packets+=1
    origin.inbox.append(msg)


def schedual_generator(duration,number_of_messages,resolution):
    #duration is the number of ms to make the timeline
    #all of the odds are in terms of % likelyhood per ms
    #resolution is the number of ms to skip per event (large numbers means many events at once)
    timeline = []
    count = numpy.arange(0.0,duration,resolution)
    for i in count:
    	for j in range(0,number_of_messages/len(count)):
    		for k in sim.files:
    			x = (i,send_request,)
    			timeline.append(x)


    return timeline


def setup_simulation(size,number_of_files,number_of_messages):        
    sim.packets = 0
    for i in range(0,number_of_files):
        f= random.randint(0,2**sim.m)
        sim.files.append(f)
        sim.replica_timeout[f] = random.triangular(sim.replica_timeout_min, sim.replica_timeout_max, sim.replica_timeout_mean)
        sim.file_gen[f] = 0.0



    sim.Topo = Chord_Topology(size)
    if(sim.IRM_IS_ON):
        for n in sim.nodes:
            sim.IRM_Nodes.append(IRM_Node(n))
    sim.event_schedual = schedual_generator(100.0,number_of_messages,1.0)
    #setup inital packets


samples = 5
for k in range(0,samples):
    sim = Simulator()
    setup_simulation(250,50,100)
    
    
    
    sim.run()
    average_length = float(sim.total_hops)/float(sim.packets)
    
    print ""
    if(sim.IRM_IS_ON):
        print "IRM IS ON"
    if(sim.conditional_polling):
        print "Conditional Polling is ON"
    
    print "min file change rate:"+str(sim.replica_timeout_min )+" + Max file change rate:"+str(sim.replica_timeout_max )+" + mean file change rate: "+str(sim.replica_timeout_mean)
    print str(len(sim.nodes))+" nodes and "+str(len(sim.files))+" files"
    print "Hops per packet   =",average_length
    print "Number of packets =",sim.packets
    print "replicas per node:"+str(sim.replica_total/(1.0*len(sim.nodes)))
    print "files: "+str(len(sim.files))
    print sim.replica_total
    print "query based replicas:"+str(sim.query_replicas)
    print "forward based replicas: "+str(sim.forward_replicas)
    print "hit rate: "+str(sim.hit_rate_sucess)+"/"+str(sim.hit_rate_total)
    print sim.hit_rate_sucess/sim.hit_rate_total
    print "number of polling messages: "+str(sim.poll_messages)
    print "number of polls intercepted by replicas:" +str(sim.intercepted_polls)
    print "number of polling/polling response hops: "+str(sim.special_hops)
    print "hop per packet: "+str(sim.special_hops/(sim.poll_messages*2.0))
    print "fraction of GETs responded to by replicas: "+str(sim.replica_reponses)+"/"+str(sim.total_reponses)
# for n in sim.nodes:
#     print "node:"+str(n.hashid)
#     for j in range(0,sim.m+1):
#         x = (n.hashid+2**j)%(2**sim.m)
#         print n.best_route(x)
#     print ""


#