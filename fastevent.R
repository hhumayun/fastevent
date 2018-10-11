library(data.table)



# The purpose of these functions is to provide utility functions 
# to manipulate grouped event/timeseries data, i.e. for instance 
# where we have purhcase/events histories of multiple consumers

# These functions are quite  fast, even for relatively large datasets 
# because they explot data.tables capability to make updats in place 

# Note that  operations are performed by group, with the grouping
# key specified as email. If your identification key is something else,
# feel free to change this




# This function sets the time to an event
# dt - dataset (should be a data table)
# condition - The condition should evaluate to true at events in 
#             the series to which we want to set the event name  
#             e.g. if we want to set the time to Purchase events 
#             then condition would be a boolean vector which would be 
#             true at event_type="Purchase".
#             
#             If we were using data.table we could specify this as follows :
#             condition = m_t[,event_type=="Purchase"]
#
# event_name is what to name the event  

set_time_to_event = function(dt,condition, event_name) {
  
  # need to set order to negative so that we have 
  setorder(dt, email, -event_date)
  dt[, recnum_grp:=seq_len(.N)]
  
  # store position of event (within email group) in lasteventidx where 
  # condition evaluates to true (e.g. at event_type=="Purchase" etc.)
  
  dt[condition, lasteventidx:=seq_len(.N), by=email]
  
  
  # store position of event all data where condition 
  # condition evaluates to true (e.g. at event_type=="Purchase" etc.)
  
  dt[, lasteventidx2:=as.integer(!is.na(lasteventidx)) * seq_len(.N), by=email]
  
  
  # store position of event all data where condition 
  # condition evaluates to true (e.g. at event_type=="Purchase" etc.)
  dt[, lasteventidx2_ng:=as.integer(!is.na(lasteventidx)) *.I]
  
  # store in cm_ng the last position in data where condition 
  # evaluates to true
  # This is kind of like a fill down in excel 
  dt[, cm_ng:=cummax(lasteventidx2_ng), by=email]
  
  #Assign indexes 
  idxinc=dt[cm_ng>0,recnum_grp]
  idxdt=dt[cm_ng>0,cm_ng]
  
  fieldname_last_ev_dt=paste(event_name,"_last_date",sep="")
  dt[,(fieldname_last_ev_dt):=as.Date(NA)]
  
  #set the next  date of event
  dt[idxinc,(fieldname_last_ev_dt):=dt[idxdt, event_date]]
  
  fieldname=paste("time_to_",event_name,sep="")
  
  dt[idxinc,(fieldname):=dt[idxdt, event_date]]
  
  # set time to next event 
  dt[,(fieldname):=as.integer(event_date-get(fieldname_last_ev_dt))]
  
  #erase temp columns
  dt[,lasteventidx:=NULL]
  dt[,lasteventidx2:=NULL]
  dt[, lasteventidx2_ng:=NULL]
  dt[, cm_ng:=NULL]

}


# This function is the same as the previous time_since_event function but it 
# adds another parameter called 'end_condition'
# The purpose of this parameter is to specify where to end the output of the time_since  
# calculation.

# An example where this is useful is the following scenario :

# Suppose we want to add the time since a customer Early Terminates aproduct to the data. an individual makes a purchase with  
# on 1/1/2015 and Early Terminates on 2016-1-1 - subsequently they make another purchase on 2016-5-1 
# it does not make sense for the time_since calculation to output a value after the next purchase - i.e. 
# after they have early terminated their previous contract and then made another purchase it does not make sense 
# to have the time since early termination of the previous purchase record carry forward. 

# Hence in this case we specify both a start condition and en end conditon - the end_condition in this case 
# specifies where we should stop outputing the time_since the events specified in the start_condition
# start_condition - a vector which is true at positions (or events) from which we would like to calculate the 
#                   time since those events occured 
# end_condition - a vector similar in form to start_condition that is true at positions after which we 
#                  want to stop outputting the time_since of the previous event in the start_condition vector

# event_name - the name we want to give the field

# To fully get a feel for what this function is doing do the calculations in the code below 
# in a spreadsheet

set_time_since_nth_event = function(dt,start_condition, end_condition, event_name) {
  setorder(dt, email, event_date)
  # Mark start and end events and number them within each email group
  dt[, recnum_grp:=seq_len(.N)]
  dt[start_condition, start_event_number:=seq_len(.N), by=email]
  dt[end_condition, end_event_number:=seq_len(.N), by=email]
  
  dt[is.na(start_event_number),start_event_number:=0]
  
  # store start_event rownumbers in start_event_idx_group and start_event_idx_all, by group and for all dataset
  dt[, start_event_idx_group:=as.integer(start_event_number!=0) * seq_len(.N), by=email]
  dt[, start_event_idx_all:=as.integer(start_event_number!=0) *.I]
  
  # store end_event rownumbers in start_event_idx_group and start_event_idx_all, by group and for all dataset
  dt[, end_event_idx_group:=as.integer(!is.na(end_event_number)) * seq_len(.N), by=email]
  dt[, end_event_idx_all:=as.integer(!is.na(end_event_number)) *.I]
  
  
  dt[is.na(end_event_number), end_event_number:=0]
  
  # at any point in time how many "Start" events have we seen so far
  dt[, start_event_number_cummax:=cummax(start_event_number), by=email]
  # at any point in time how many "End" events have we seen so far
  dt[, end_event_number_cummax:=cummax(end_event_number), by=email]
  
  
  # while this condition is true we will set the output to 1 else suppress output
  dt[,output_event:=as.integer(NA)]
  
  
  #while the number of start events is greater than the number of end events we will output 1
  dt[start_event_number_cummax>end_event_number_cummax,output_event:=as.integer(1)]
  
  #or while the number of end events is zero or na (i.e. no end events have occurred yet) we 
  #will also output 1
  dt[is.na(end_event_number_cummax) | (end_event_number_cummax==0) ,output_event:=as.integer(1)]
  
  
  #Store the last start event number idx in cm_ng 
  dt[, cm_ng:=cummax(start_event_idx_all), by=email]
  
  # get index numbers where we want to calculate time since for entire dataset and 
  # by group
  idxinc=dt[cm_ng>0,recnum_grp]
  idxdt=dt[cm_ng>0,cm_ng]
  
  fieldname_last_ev_dt=paste(event_name,"_last_date",sep="")
  dt[,(fieldname_last_ev_dt):=as.Date(NA)]
  
  #set dates of the last event seen
  dt[idxinc,(fieldname_last_ev_dt):=dt[idxdt, event_date]]
  
  fieldname=paste("time_since_",event_name,sep="")
  
  dt[,(fieldname):=as.integer(NA)]
  dt[idxinc,(fieldname):=dt[idxdt, event_date]]
  dt[idxinc,(fieldname):=dt[idxdt, event_date]]
  
  #calculate the time since the last event 
  dt[,(fieldname):=as.integer(event_date-get(fieldname_last_ev_dt))]
  
  
  #if output event is 1 or NA we would like to suppreess the output
  dt[!(output_event==1), (fieldname):=as.integer(NA)]
  dt[is.na(output_event), (fieldname):=as.integer(NA)]
  
  # Clear temporary columns 
  dt[,end_event_number_cummax:=NULL]
  dt[,start_event_number:=NULL]
  dt[,end_event_number:=NULL]
  dt[,start_event_valid_till:=NULL]
  dt[,start_event_idx_group:=NULL]
  dt[,end_event_idx_group:=NULL]
  dt[,end_event_idx_all:=NULL]
  dt[,start_event_idx_all:=NULL]
  dt[,cm_ng:=NULL]
  dt[,output_event:=NULL]
  dt[,start_event_number_cummax:=NULL]
  dt[,(fieldname_last_ev_dt):=NULL]
  
}


# This function sets the time since an event has occurred
# dt - data (as data.table )
# condition - this should be a vector that is true at records for which
#             we want to evaluate the time since an event

set_time_since_event = function(dt,condition, event_name) {
  dt[, recnum_grp:=seq_len(.N)]
  
  
  dt[condition, lasteventidx:=seq_len(.N), by=email]
  
  # set the index(or position) (within a group) where the condition 
  # evaluates to true. 
  dt[, lasteventidx2:=as.integer(!is.na(lasteventidx)) * seq_len(.N), by=email]
  
  # set the index(or position) (within total dataset) where the condition 
  # evaluates to true. 
  dt[, lasteventidx2_ng:=as.integer(!is.na(lasteventidx)) *.I]
  
  # this is the position (wihtin group) of the last event 
  dt[, cm_ng:=cummax(lasteventidx2_ng), by=email]
  
  # idxinc is the position within the whole dataset 
  # where we want to find the time since event
  
  # idxdt is the index wihtin the group where we want to 
  # set the time since event 
  idxinc=dt[cm_ng>0,recnum_grp]
  idxdt=dt[cm_ng>0,cm_ng]
  
  
  fieldname_last_ev_dt=paste(event_name,"_last_date",sep="")
  dt[,(fieldname_last_ev_dt):=as.Date(NA)]
  
  #get the rolling last event date where the event date occurred 
  dt[idxinc,(fieldname_last_ev_dt):=dt[idxdt, event_date]]
  
  fieldname=paste("time_since_",event_name,sep="")
  
  # set the event date
  dt[idxinc,(fieldname):=dt[idxdt, event_date]]
  
  # subtract event date from the last time this event event occured 
  # to get time since the event occurred 
  dt[,(fieldname):=as.integer(event_date-get(fieldname_last_ev_dt))]
  
  #erase temp columns
  dt[,lasteventidx:=NULL]
  dt[,lasteventidx2:=NULL]
  dt[, lasteventidx2_ng:=NULL]
  dt[, cm_ng:=NULL]
}



# This function 'fills-down' the value of the last non-empty value by group  (e.g. as done in excel) to the next 
# non-empty value 

#condition - a vector that is True at position which we would like to fill down
#fieladname - the name of the column that will be fileld down 
#event_name - this will be the name of the new column created 

set_field_val_last_ev = function(dt, condition, field_name, event_name){
  setorder(dt, email, event_date)
  # assign row number to each record 
  dt[, recnum_grp:=seq_len(.N)]
  
  
  #Now set record numbers by group only for those records that meet the condition
  #in condition vector. This will number each record where condition is true
  #by 1...Nc where Nc is the number of conditions that are true in group
  dt[condition, lasteventidx:=seq_len(.N), by=email]
  
  #Now set group record number for records where condition is true 
  dt[, lasteventidx2:=as.integer(!is.na(lasteventidx)) * seq_len(.N), by=email]
  
  #Now set dataset record number for records where condition is true 
  dt[, lasteventidx2_ng:=as.integer(!is.na(lasteventidx)) *.I]
  
  #Store the last record number in cm_ng 
  dt[, cm_ng:=cummax(lasteventidx2_ng), by=email]
  
  #These are the rowids for where an event has occurred in 
  #the past 
  idxinc=dt[cm_ng>0,recnum_grp]
  
  #These are the cumulative max rowids for the last event 
  idxdt=dt[cm_ng>0,cm_ng]
  
  fieldname_last_ev_val=paste(event_name,"_last_val",sep="")
  dt[,(fieldname_last_ev_val):=NA]
  
  #Replace cumulative maximum by last maximum ?
  dt[idxinc,(fieldname_last_ev_val):=dt[idxdt, field_name, with=FALSE]]
  
  fieldname_use=paste("last_val_",event_name,sep="")
  
  dt[idxinc,(fieldname_use):=dt[idxdt, field_name, with=FALSE]]
  #dt[,(fieldname_use):=as.integer(event_date-get(fieldname_last_ev_dt))]
  
  #erase temp columns
  dt[,(fieldname_last_ev_val):=NULL]
  dt[,lasteventidx:=NULL]
  dt[,lasteventidx2:=NULL]
  dt[, lasteventidx2_ng:=NULL]
  dt[, cm_ng:=NULL]
  return(dt)
}


# This function is similar to the field_val_last_ev function but adds the condition of an end_condition. The end condition is true at indexes after which we would not like to 'fill-down' the field in field_name. 
# The reason for this functionality is the following : Consider a person who first buys on finance but then 
# after maturity they Purchase without finance. THen we would not like to output (or 'fill-down') values like outstanding balance 
# after the second purchase since they are no longer relevant. If we just used the previous funciton without the end condition, these fields would continue to be filled-down. 

# start_condition vector of booleans that is true at positions where we want to 

set_field_val_nth_last_ev = function(dt, start_condition, end_condition, field_name, event_name){
  
  setorder(dt, email, event_date)
  # Marke start and end events and number them 
  dt[, recnum_grp:=seq_len(.N)]
  dt[start_condition, start_event_number:=seq_len(.N), by=email]
  dt[end_condition, end_event_number:=seq_len(.N), by=email]
  
  dt[is.na(start_event_number),start_event_number:=0]
  
  # start event rownumbers (by group and all dataset)
  dt[, start_event_idx_group:=as.integer(start_event_number!=0) * seq_len(.N), by=email]
  dt[, start_event_idx_all:=as.integer(start_event_number!=0) *.I]
  
  # end event rownumbers (by group and all dataset)
  dt[, end_event_idx_group:=as.integer(!is.na(end_event_number)) * seq_len(.N), by=email]
  dt[, end_event_idx_all:=as.integer(!is.na(end_event_number)) *.I]
  
  
  dt[is.na(end_event_number), end_event_number:=0]
  
  # at any point in time how many "Start" events have we seen 
  dt[, start_event_number_cummax:=cummax(start_event_number), by=email]
  # at any point in time how many "End" events have we seen 
  dt[, end_event_number_cummax:=cummax(end_event_number), by=email]
  
  
  # while this condition is true we will set the output to 1 else suppress output
  dt[,output_event:=as.integer(NA)]
  dt[start_event_number_cummax>end_event_number_cummax,output_event:=as.integer(1)]
  dt[is.na(end_event_number_cummax) | (end_event_number_cummax==0) ,output_event:=as.integer(1)]
  
  
  
  
  #Store the last start event number idx in cm_ng 
  dt[, cm_ng:=cummax(start_event_idx_all), by=email]
  
  
  idxinc=dt[cm_ng>0,recnum_grp]
  idxdt=dt[cm_ng>0,cm_ng]
  fieldname_last_ev_val=paste(event_name,"_last_val",sep="")
  dt[idxinc,(fieldname_last_ev_val):=dt[idxdt, field_name, with=FALSE]]
  
  fieldname_use=paste("last_val_",event_name,sep="")
  
  dt[idxinc,(fieldname_use):=dt[idxdt, field_name, with=FALSE]]
  dt[!(output_event==1), (fieldname_use):=as.integer(NA)]
  dt[is.na(output_event), (fieldname_use):=as.integer(NA)]
  
  
  dt[,start_event_number:=NULL]
  dt[,start_end_number:=NULL]
  dt[,start_event_number_cummax:=NULL]
  dt[,end_event_number_cummax:=NULL]
  dt[,end_event_number:=NULL]
  dt[,start_event_valid_till:=NULL]
  dt[,start_event_idx_group:=NULL]
  dt[,end_event_idx_group:=NULL]
  dt[,end_event_idx_all:=NULL]
  dt[,start_event_idx_all:=NULL]
  dt[,cm_ng:=NULL]
  dt[,output_event:=NULL]
  dt[,(fieldname_last_ev_val):=NULL]
  
  #dt[!(output_event==1), (fieldname):=NA]
  #dt[is.na(output_event) , (fieldname):=NA]
  
  #erase temp columns
  
}


# This function takes a dataset and counts the number of events at each point 
# where condition evaluates to true. For example to count the number of purchases 
# we would set condition = m_t[,event_type=="Purchase"] to get number of purchases 

# dt - the datatable containing the data 
# condition - this should evaluate to true for events that we want to count 
# event_name -  this is the field name that we want to set 

# time_window is the number of months look back at
# NOTE::: Time window functionality is not working yet 
set_number_events = function(dt, condition, event_name, time_window=6){
  dt[,lasteventidx:=as.integer(0)]
  dt[condition, lasteventidx:=seq_len(.N), by=email]
  
  # This identifies events based where condition is TRUE
  # and then uses that to number events within the
  # group
  
  fieldname=paste(event_name,"_num_sofar",sep="")
  
  # At each point calculate the total number of events seen
  # so far
  dt[, (fieldname):=cummax(lasteventidx), by=email]
  
  dt[,lasteventidx:=NULL]
  return(dt)
}


# Sum values in the field 'field' by email 


set_sum_val = function(dt, condition, field, event_name){
  dt[,lasteventidx:=as.integer(0)]
  dt[condition, lasteventidx:=seq_len(.N), by=email]
  dt[condition, lasteventsum:=lapply(.SD, cumsum),.SDcols=(field), by=email]
  
  dt[condition , lasteventsum:=lapply(.SD, cumsum),.SDcols=(field), by=email]
  
  # This identifies events based where condition is TRUE
  # and then uses that to number events within the
  # group
  
  fieldname=paste(event_name,"_sum_sofar",sep="")
  
  # At each point calculate the sum  seen
  # so far
  dt[, (fieldname):=lasteventsum, by=email]
  
  dt[,lasteventidx:=NULL]
  dt[,lasteventsum:=NULL]
  return(dt)
}



