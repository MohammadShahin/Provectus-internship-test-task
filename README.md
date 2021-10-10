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

1. ```
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
    return count```
2. ```
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
    return ans```


<a name="linux-shell"></a>
### Linux shell
1. ```$

<a name="author"></a>
## Author
