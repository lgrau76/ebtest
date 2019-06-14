# batch process earlyTest
1 input file inpu.csv (userId,itemId,rating,timestamp)
3 output files : 
aggratings.csv : userIdAsInteger,itemIdAsInteger,ratingSum (rating*0.95^day/(max(timestamp)-timestamp) and filter>0.01)
lookupuser.csv : userId,userIdAsInteger 
lookup_product.csv : itemId,itemIdAsInte