import Pyro4
import sys
import time
import random
import threading

nodename = sys.argv[1]


class Node(threading.Thread):
    def __init__(self,pyroname=None,pyromember=None):
        threading.Thread.__init__(self)
        self.name=''
        if (pyroname is not None):
            self.state='follower'
            self.leader=None
            self.currentTerm=1
            self.commitIndex=0
            self.name=pyroname
            self.data={}
            self.votedFor=self.name
            self.commitIndex=0
            self.electionAlarm=random.randint(10,20)
            #self.proxy = Pyro4.Proxy(self.name)
            if (pyromember is not None):
                self.member = pyromember
                self.proxymember={}
                for xx in self.member:
                    self.proxymember[xx]={ 'nextIndex':1, 'matchIndex':0, 'granted': False, 'service': Pyro4.Proxy(xx)}
    def setState(self,status='follower'):
        print "set state {}" . format(status)
        self.state=status
    def getState(self):
        return self.state
    def setData(self,index=0,data=None):
        if (data is not None):
            self.data[index]=data
    def getData(self,index=0):
        try:
            return self.data[index]
        except KeyError:
            return None
    def getAllData(self):
        return self.data
    def incrCommitIndex(self):
        self.commitIndex=self.commitIndex+1
    def setVotedFor(self,name=None):
        if (name is not None):
            self.votedFor=name
    def setCurrentTerm(self,term):
        print "set current term {}" . format(term)
        self.currentTerm=term
    def resetElectionAlarm(self):
        print "reset election {}" . format(self.name)
        self.electionAlarm = random.randint(10,20)
    def run(self):
        while (True):
            time.sleep(1)
            self.electionAlarm=self.electionAlarm-1
            print "\r{} alarm {} state {} votedFor {} commitIndex {} currentTerm {}". format(self.name,self.electionAlarm,self.state,self.votedFor,self.commitIndex,self.currentTerm)
            if (self.state=='follower'):
                if (self.electionAlarm>0):
                    continue
                self.state='candidate'
                self.resetElectionAlarm()
                continue
            elif (self.state=='candidate'):
                #if (self.electionAlarm>0):
                #    continue
                self.currentTerm=self.currentTerm+1
                rep={}
                jum=0.0
                for xx in self.proxymember:
                    if xx==self.name:
                        continue
                    try:
                        rep[xx] = self.proxymember[xx]['service'].requestVote(self.currentTerm,self.name)
                        if (rep[xx]['granted']==True):
                            self.proxymember[xx]['granted']=True
                            jum=jum+1
                    except Exception as xx:
                        #print xx
                        print "1 error on connecting {}" . format(xx)
                rasio = float(jum) / len(self.proxymember)
                print "rasio {:f} {}/{}" . format(rasio,jum,len(self.proxymember))
                if ((rasio>=0.5) and (rasio<1)):
                    self.state='leader'
                continue
            elif (self.state=='leader'):
                print "I AM LEADER"
                rep2={}
                jum2=0.0
                for xx in self.proxymember:
                    if xx==self.name:
                        continue
                    try:
                        rep2[xx] = self.proxymember[xx]['service'].appendEntries(self.getData(self.commitIndex),self.currentTerm,self.commitIndex)
                        if (rep2[xx]['success']==True):
                            jum2=jum+1
                    except Exception as dd:
                        print dd
                        print "2 error on connecting {}" . format(xx)
                rasio2 = float(jum2) / len(self.proxymember)
                print "rasio {:f} {}/{}" . format(rasio2,jum2,len(self.proxymember))
                #if ((rasio2>=0.5) and (rasio2<1)):
                #    self.state='leader'

fp=open('cluster')
members=fp.readlines()
members=[m.strip() for m in members]
myname = 'PYRONAME:{}' . format(nodename)
t1=Node(myname,members)
t1.daemon=True

@Pyro4.expose
class NodeServer(object):
    def __init__(self):
        self.data={}
    def 
    def requestVote(self,term=0,name=None):
        print "get request vote"
        t1.resetElectionAlarm()
        t1.setCurrentTerm(term)
        t1.setVotedFor(name)
        t1.setState('follower')
        return { 'term': term, 'granted': True, 'votedFor': name }
    def appendEntries(self,data=None,term=0,commitIndex=0):
        print "adding {}" . format(data)
        t1.resetElectionAlarm()
        t1.setCurrentTerm(term)
        t1.setState('follower')
        if (data is not None):
            self.data[commitIndex]=data
        else:
            print "no data"
        return { 'term': term, 'success': True, 'matchIndex':commitIndex}




d = Pyro4.Daemon()
ns = Pyro4.locateNS()
uri = d.register(NodeServer)
ns.register("{}" . format(nodename),uri)
print "nama node {} uri {}" . format(nodename,uri)


t1.start()
d.requestLoop()

