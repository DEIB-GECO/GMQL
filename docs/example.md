
# GMQL Engine V2.1
## Deployment
GMQL Engine has several deployment modes: 

### Shell deployment without Repository.
  
  Example Code:
  
#### Local Execution (single machine)
  ```
    A = SELECT() /home/user1/ds/ref/;
    B = SELECT() /home/user1/ds/exp/;
    S = MAP() A B;
    MATERIALIZE S INTO /home/user1/ds/out/S/;
  ```
   
#### Yarn Execution (cluster)
  
  ```
    A = SELECT() hdfs://127.0.0.1:9000/user/repo/user1/regions/ref/;
    B = SELECT() hdfs://127.0.0.1:9000/user/repo/user1/regions/exp/;
    S = MAP() A B;
    MATERIALIZE S INTO hdfs://127.0.0.1:9000/user/repo/user1/regions/out/;
  ```
  
### Shell Deployment with Repository.

  Example Code: 
  
 ```
    A = SELECT() ann;
    B = SELECT() exp;
    S = MAP() A B;
    MATERIALIZE S INTO res;
  ```
