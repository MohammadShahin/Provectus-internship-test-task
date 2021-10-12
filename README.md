# Provectus-Internship
This file explains and goes through the steps of Provectus Internship's test task's 
solution. 

## Table of contents
- [Data Processing Problem level 2](#data-processing-problem)
  - [Tech stack](#tech-stack)
  - [Logic](#logic)
  - [Database](#database)
  - [Running this app](#run-app)
- [Coding Tasks for Data Engineers](#coding-tasks-for-data-engineers)
  - [SQL](#sql)
  - [Algorithms and Data Structures](#dsa)
  - [Linux shell](#linux-shell)
- [Author](#author)

<a name="data-processing-problem"></a>
## Data Processing Problem level 2

<a name="tech-stack"></a>
### Tech stack
The implementation was done using python with Flask as the main service. Postgres SQL was used as the main database alongside with the output csv file to store the data. MinIO was used to manage the file system.


<a name="database"></a>
### Database
The postgres database consists of one table which is the *users*. The table includes the following columns: {id, user_id, first_name, second_name, birthts, img_path}. The columns labels are self explanatory. Please note that when starting a new data processing operation, unlike the *output.csv* file, the database does not drop its rows. It just updates the existing ones or adds new ones. Currently, no function was added to drop a row from the *users* table.

<a name="logic"></a>
### Logic
The script *data_processing/main.py* contains the main functionality for processing the data from the *srcdata* bucket and updating *output.csv* and the postgres database with results. It interacts with MinIO and postgres through *data_processing/minio_handler.py* and *data_processing/postgres_handler.py*. Firstly, it initiates the database, clears the file *output.csv* and writes only the names of columns in it. Secondly, it generates a list of all the csv files located in the *srcdata* directory. Then it goes through each csv file and processes it independently. The processing of each csv file can be explained in the following steps:
1. Checking the validity of the csv file contents: if it contains exactly the expected columns, one row for values, the types of the values match their columns, etc. If the csv file is valid, then process goes on to step 2. Otherwise, the process of handling this csv file is aborted.
2. It checks if a matching image file with the csv file exists in the *srcdata* directory. Please note that the absence of such an image does not mean aborting the csv file processing.
3. Finally it writes the results to *output.csv* and the postgres database. If an image was not found in the previous step, then the value at *img_path*'s column will be simple empty (''). 

The Flask service, which is managed by *app.py*, contains mainly three main functions described below:
1. An endpoint **GET** /data - get all records from DB in JSON format. Need to implement filtering by: is_image_exists = True/False, user min_age and max_age in years. The response for this query is found by first generating the conditions of the existing arguments, then by getting all the rows in the *users* table as a dictionary, and finally filter the dictionary and return it. 
2. **POST** /data - manually run data processing in src_data. In this process, both the *output.csv* file and the postgres database are upadated.
3. Periodically run data processing in src_data. This was done using multiprocessing. A new process is created to apply periodic update of the *output.csv* and the postgres database every 15 minutes.


<a name="run-app"></a>
### Running this app
This part explain the steps of running the app on Ubuntu 20.04.3 or higher with python 3.8.10 or higher. 

Please note that the docker-compose was modified to include the Flask web service. This was done to make running the app process easier and shorter. Also note that a *Dockerfile* was added to help facilitate the Flask running.

Now please follow the next steps to run the app:
1. First you need to build the docker-compose using the following command:
```
$ sudo docker-compose up --build
```
This will generate some new directories in the project. Moreover, this command will result in some errors because there was no permission granted for the data processing to edit *minio* and *pgadmin* (new directories). To change that you need first to shut down docker-compose by keyboard interrupt and run the command:
```
$ sudo docker-compose down
```
2. You need now to change the permissions in *minio* and *pgadmin*. You can do that using the following commands:
```
$ sudo chmod 777 minio/
$ sudo chmod 777 pgadmin/
```
3. Now run build docker-compose again, and it should work with no errors:
```
$ sudo docker-compose up --build
```
However since the *srcdata* directory in *minio* is empty, there will nothing to process. Thus to fix this issue, shut down docker again just like we did in step 1 and then you need to change the permission of *srcdata*. You need to run the following command in *minio* directory:
```
$ sudo chmod 777 srcdata/
```
Then copy all of the files you want to process and paste them in *srcdata*. Finally run build docker-compose:
```
$ sudo docker-compose up --build
```
This will make the app run localhost:3001. The results will be generated and stored in *output.csv* in *processeddata* in *minio* directory alongside the postgres database.

<a name="coding-tasks-for-data-engineers"></a>
## Coding Tasks for Data Engineers

<a name="sql"></a>
### SQL

1. 
```
SELECT users.id as id FROM 
users LEFT JOIN departments 
ON users.id = departments.user_id
WHERE departments.user_id is NULL OR departments.department_id != 1;
```
2. 
``` 
SELECT last_name FROM user 
GROUP BY last_name 
HAVING COUNT(last_name) > 1
```
3. 
```
SELECT user.username as username, salary.salary as salary FROM user, salary 
WHERE user.id = salary.user_id AND user.id IN (
SELECT user_id FROM salary 
ORDER BY salary DESC LIMIT 1,1
);
```

<a name="dsa"></a>
### Algorithms and Data Structures

1. Optimise execution time of this Python code snippet:
```
def count_connections(list1: list, list2: list) -> int:
  count = 0
  
  for i in list1:
    for j in list2:
      if i == j:
        count += 1
  
  return count
```
  
  
Solution: instead of iterating on each pair of values in list1 and list2, we simply create a counting dictionary, which just counts the number of occurances of each value in the first list (this dictionary can be constructed in O(len(list1)) assuming the dictionary operations takes O(1) on average). Then we go through the elements of the second list, and for each element `elm` we add `counting_dictionary[elm]` to our final answer (this also takes O(len(list2)) assuming the dictionary operations takes O(1) on average). Thus we reduced the overall complexity from O(len(list1) * len(list2)) to O(len(list1) + len(list2)). The space complexity is O(len(list1)). 
The solution's implementation:
```
def count_connections2(list1: list, list2: list) -> int:
    count = 0
    count_dict = {}
    for i in list1:
        if not count_dict.get(i, False):
            count_dict[i] = 0
        count_dict[i] += 1
    for j in list2:
        if count_dict.get(j, False):
            count += count_dict[j]
    return count
```

2. Given a string s, find the length of the longest substring without repeating characters. Analyze your solution and please provide Space and Time complexities.

Solution: let's say we are currently dealing with substring starting from `left` and ending with `right` (`left` and `right` are indices where `left`, `right` <= `len(s)`) and it is currently a valid substring (all its character are distinct). Let's keep a dictionary `char_index_dict` which matches each character present in the substring\[`left`, `right`] with its index (a number in the range \[`left`, `right`]). Now let's say we want to fix `left` and find the optimal (maximum) `right` which keeps the substring valid. What we need to do is iteratively check if including the character `char` with index `right + 1` keeps the substring valid (we can do this by checking if `char` exists in `char_index_dict`) and then setting `right` to `right + 1`. If the new character (`char`) was already present in the dictionary, then we have found the maximum `right` for the `left`. Now ley's define `index_char = char_index_dict[char]`, we know that for each `left2` in the range \[`left, index_char`] 
In the solution, we iteratre over each character in the string twice at maximum, assuming the dictionary operations takes O(1) on average, the overall time complexity will be O(len(s)). We are storing the `char_index_dict` dictionary, thus at worst case (all characters are distinct), the space complexity is O(len(s)).
The solution's implementation:
```
def longest_substring_without_repeating(s):
    char_index_dict = {}
    ans = 0
    left = 0
    for index in range(len(s)):
        char = s[index]
        if not char_index_dict.get(char, False):
            char_index_dict[char] = index + 1
        else:
            temp = char_index_dict[char]
            for j in range(left, temp):
                char_index_dict.pop(s[j])
            char_index_dict[char] = index + 1
            left = temp
        ans = max(ans, len(char_index_dict))
    return ans


print(longest_substring_without_repeating('abcabcbb')) # 3
print(longest_substring_without_repeating('bbbbb')) # 1
print(longest_substring_without_repeating('pwwkew')) # 3 
print(longest_substring_without_repeating('')) # 0
```
3. Given a sorted array of distinct integers and a target value, return the index if the target is found. If not, return the index where it would be if it were inserted in order.

Solution: to solve this problem, we can use binary search. We start by setting `left = 0` and `right = len(array) - 1`, then we check the middle value between left and right and update them accordingly by either setting left or right to the middle index. We keep doing this until we find the upperbound index of the target, i.e. the smallest index whose element is larger or equal to the target value. The time complexity of the solution is O(log(n)) and the space complexity is O(1) since only a couple of new variables were introduced.
The solution's implementation:

```
def locate_target(nums: list, target: int):
    left = 0
    right = len(nums) - 1
    index = -1
    while left <= right:
        mid = (left + right) // 2
        if nums[mid] < target:
            left = mid + 1
        else:
            index = mid
            right = mid - 1
    if index == -1:
        return len(nums)
    return index


print(locate_target(nums=[1, 3, 5, 6], target=10)) # 2
```


<a name="linux-shell"></a>
### Linux shell
1. List processes listening on ports 80 and 443.
```$ sudo netstat -lnp | egrep -w ':80|433' ```

2. List process environment variables by given PID
``` $ sudo tr '\0' '\n' < /proc/<PID>/environ```

3. Launch a python program my_program.py through CLI in the background. How would you close it after some period of time?
To run the program in the background, we need to run the following command: ```nohup python /the/path/to/my_program.py &```. This will make the file ignore the inputs and redirect all of the program's output (stdout, stderr) to a file nohup.out. 
Now to close this program, we need to find the PIDs (processes' id) associated with the program. This can done using the following command: ```ps ax | grep my_program.py``` Then we need to kill these processes by running ```kill PID``` of each of the processes. Finally we can combine the previous commands in one which will be as the following: ```kill $(ps aux | grep 'python my_program.py' | awk '{print $2}')``` (`awk '{print $2}'` is used for getting the second value in the processes rows, which is the PID). 

<a name="author"></a>
## Author
Mohammad Shahin, a third-year student at Innopolis University.
