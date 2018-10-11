# fastevent


### Fast Temporal Aggregations for Event Data 
This is a collection of functions that can be used to add temporal statistics to data that has multiple groups. For instance if there is event data by multiple consumers in the data.

These functions are quite  fast, even for relatively large datasets because they exploit data.tables capability to make updates in place. Note that  operations are performed by group, with the grouping key currently called "email". If your identification key is something else, feel free to find/replace "email" in the code and replace with the name of your own key.

#### Important: The dataset needs to be temporally sorted within each group (e.g. if consumer data then each consumer's event data must be sorted)
