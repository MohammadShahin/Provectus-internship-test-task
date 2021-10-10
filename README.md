# Provectus-Internship
This file explains and goes through the steps of Provectus Internship's test task's 
solution. 

## Table of contents
- [Data Processing Problem level 2](#data-Processing-Problem-level-2)
  - [Tech stack](#tech-stack)
  - [Running this app](#running-this-app)
- [Coding Tasks for Data Engineers](#coding-tasks-for-data-engineers)
  - [SQL](#sql)
  - [Algorithms and Data Structures](#dsa)
  - [Linux shell](#linux-shell)
- [Author](#author)

<a name="coding-tasks-for-data-engineers"></a>
## Data Processing Problem level 2

### Tech stack

<a name="running-this-app"></a>
### Running this app

<a name="coding-tasks-for-data-engineers"></a>
## Coding Tasks for Data Engineers

<a name="sql"></a>
### SQL

1. asad
2. ``` SELECT * FROM user GROUP BY last_name HAVING COUNT(last_name) > 1```
3. ```SELECT * FROM  group by salary order by  salary desc limit 1,1;```

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
3. 


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
Mohammad Shahin, a third year student at Innopolis University.
