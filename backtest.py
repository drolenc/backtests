#!/usr/bin/python

import sys
import os
import _mysql
import time
import calendar
import datetime
import sets

outf = open("backtest.csv", "w+")
print >>outf,"SYM,1S,2S,SAMPLE1,SAMPLE2"     


def processDate(thedate):
   sdate = thedate.split("/")
   if(len(sdate) != 3):
      print "Error in date"
      sys.exit(-1);
   m = int(sdate[0])
   d = int(sdate[1])
   y = int(sdate[2])

   if(y < 2000):
      y += 2000;

   dt = datetime.datetime(y, m, d)
   t = calendar.timegm(dt.timetuple())
   return t

def showDate(thedate):
   return time.strftime('%m/%d/%Y', time.gmtime(thedate))

db=_mysql.connect(unix_socket="/var/lib/mysql/mysql.sock", user="root", passwd="", db="options")


sampledate = processDate(sys.argv[1])
sampledate_1y =  sampledate - (86400 * 365)

syms = []

# get all symbols in the database for the test period
stmt = "select distinct symbol from dailystk where sampledate >=  %d" % (sampledate)
db.query(stmt)
rs=db.store_result()
numrows = rs.num_rows()
if(numrows > 0):
     while (1):
         r = rs.fetch_row(1) 
         if(len(r)==0):
            break;
         syms.append(r[0][0])


#| id| symbol | optsymbol| expiry| sampledate | strike | call_put|last| bid| ask| volume | oi | iv |delta| gamma  | theta   | vega   | dte | gmean | source |

class opts:
    def __init__(self,t):
        self.symbol = t[0] 
        self.strike = float(t[1])
        self.bid = float(t[2])
        self.ask = float(t[3])
        self.gmean = float(t[4])
        self.call_put = t[5]
        self.expiry = int(t[6])
        self.dte = int(t[7])
        self.delta = float(t[8])
        self.theta = float(t[9])
        self.oi = int(t[10])
        self.sampledate = int(t[11])

    def __str__(self):
        return "SYM %s\t%s\t%s %.02f\tBID %.02f\tASK %.02f\tMEAN %.02f\tEXPIRY %s\tDTE %d\tDELTA %.02f\tTHETA %.02f OI %d" % (self.symbol, showDate(self.sampledate), self.call_put, self.strike, self.bid, self.ask, self.gmean, showDate(self.expiry), self.dte, self.delta, self.theta, self.oi)

#iterate over symbols
for s in syms:

        within_one = 0
        within_two = 0
        tries_one = 0
        tries_two = 0

        expirys = []
     
        # we want all expirations with between 4 and 7 days to go, inclusive. 
        stmt = "select DISTINCT expiry from dailyopt where symbol = '%s' and sampledate >= %d and dte <= 7 and dte >= 4" % (s,sampledate_1y)
        db.query(stmt)
        rs=db.store_result()
        numrows = rs.num_rows()
        if(numrows > 0):
           while (1):
             r = rs.fetch_row(1) 
             if(len(r)==0):
                 break;
             expirys.append(r[0][0])

       
        #each expiration gets a test 
        for expiry in expirys:

            optlist = []

            # For this expiry, grab the first sample date within <= 7days to expiration 
            sdate = None
            stmt = "select sampledate,dte from dailyopt where symbol = '%s' and sampledate >= %d and dte <= 7 and expiry = %s order by sampledate asc limit 1" % (s,sampledate_1y, expiry)
            db.query(stmt)
            rs=db.store_result()
            numrows = rs.num_rows()
            if(numrows > 0):
               while (1):
                 r = rs.fetch_row(1) 
                 if(len(r)==0):
                     break;
                 sdate = r[0][0]


            # Get options data for that sample date and expiry - that is the start of the test
            stmt = "select symbol,strike, bid, ask, gmean, call_put, expiry, dte,delta,theta,oi,sampledate from dailyopt where symbol = '%s' and sampledate = %s and expiry = %s" % (s,sdate, expiry)
            db.query(stmt)
            rs=db.store_result()
            numrows = rs.num_rows()
            if(numrows > 0):
               while (1):
                 r = rs.fetch_row(1) 
                 if(len(r)==0):
                     break;
                 O = opts(r[0])
                 optlist.append(O) 

         
  
            # get a list of strikes. 
            strikes = list(sets.Set(map(lambda(x):x.strike, optlist)))
            strikes.sort()

            # amount paid out
            max_pain = {}

            # for each strike, see what's in the money and add it to our payout map
            for st in strikes:
                max_pain[st] = 0.0
                for o in optlist:
                    if(o.call_put=='C'):
                         if(st > o.strike):
                            max_pain[st] += (((st-o.strike) * o.oi) * 100.0)
                    else:
                         if(st < o.strike):
                            max_pain[st] += (((o.strike - st) * o.oi) * 100.0)

            #now get the minimum payout from the map
            min_payout = max_pain.items() 
            min_payout.sort(key = lambda x:x[1]) 
            the_min = min_payout[0]

            #find the minimum payout strike from within the list
            #that way we can bracket around it for success criteria
            # take care not to wrap the indexes
            the_index =  strikes.index(the_min[0])
            stM1 = None
            stP1 = None
            stM2 = None
            stP2 = None
            if(the_index + 1 < len(strikes) and (the_index -1 > -1)):
                stM1 = strikes[the_index-1]
                stP1 = strikes[the_index+1]
            if(the_index + 2 < len(strikes) and (the_index -2 > -1)):
                stM2 = strikes[the_index-2]
                stP2 = strikes[the_index+2]

            
            #finally, get the close at expiration, and see if it fell within the bracketed success criteria

            stmt = "select close from dailystk where symbol = '%s' and sampledate =  %d" % (s,optlist[0].expiry)
            db.query(stmt)
            rs=db.store_result()
            numrows = rs.num_rows()
            if(numrows > 0):
                 while (1):
                     r = rs.fetch_row(1) 
                     if(len(r)==0):
                        break;
                     closing_price = float(r[0][0])
                     #print "Max Pain is %.02f Close %.02f"% (the_min[0], closing_price)
                     if(stM1 != None and stP1 != None):
                          #print "S1", stM1,stP1
                          tries_one += 1
                          if(closing_price > stM1 and closing_price < stP1):
                              within_one += 1 
                     if(stM2 != None and stP2 != None):
                          #print "S2", stM2,stP2
                          tries_two += 1
                          if(closing_price > stM2 and closing_price < stP2):
                              within_two += 1 
        w1 = "n/a"
        w2 = "n/a"
        try:
          w1 = "%.02f" % (float(within_one)/float(tries_one))
          w2 = "%.02f" % (float(within_two)/float(tries_two))
        except:
          pass

        print "SYM %s \t1S %s \t2S %s \tTries (%d, %d)" % (s,w1,w2,tries_one,tries_two)    
        print >>outf,"%s,%s,%s,%d,%d" % (s,w1,w2,tries_one,tries_two)    
             
outf.close()
