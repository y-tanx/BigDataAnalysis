# Lab3

## 数据集概述

前往SNAP（http://snap.stanford.edu/data/ego-Facebook.html）下载斯坦福提供的Facebook社交网络数据。有两个压缩包：

+ facebook.tar.gz：包含了10个网络的数据
+ facebook_combined.txt.gz：无向图的边列表

解压这两个文件，能获得facebook目录 和 facebook_combined.txt文件

+ facebook目录：包含了10个社交网络的数据，每个社交网络都是以某个用户的ID为中心点，相应的数据文件为ID.xxx

  + **ID.circles**：用户的朋友分组

    ```txt
    circle0	71	215	54	61	298	229	81	253	193	97	264	29	132	110	163	259	183	334	245	222
    ```

    每个circle代表用户ID的一个社交圈，即朋友分组。

    格式为`分组id 用户ID1 用户ID2···`

  + **ID.edges**：用户的好友关系

    ```txt
    236 186
    122 285
    24 346
    271 304
    176 9
    130 329
    204 213
    252 332
    82 65
    276 26
    ```

    每一行是一对用户的ID，表示他们之间的好友关系

    格式为`用户ID1 用户ID2`

  + **ID.egofeat**：核心用户（中心点）的特征信息

    ```txt
    0 0 0 0 0 0 0 0 0 1 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 1 0 1 1 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 1 0 0 0 0 1 0 0 0 1 0 0 1 0 0 1 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 1 0 0 0 0 0 0 0 0 0 1 1 0 1 0 1 0 1 0 0 0 0 0 0 1 0 0 0 0
    ```

    采用了0/1编码的方式指示核心用户的特征信息，这需要与ID.featnames对照来看：.egofeat文件中第`i`位为1，则在.featnames文件中找到第`i`行（从0开始计数），获得这一行对应的**特征名称**和**特征取值**。由于是匿名的，因此特征取值都是`feature xx`的形式。举个例子，上述的.egofeat文件中，第9位取到了1，在.featnames中第9行的内容：

    ```txt
    education;classes;id;anonymized feature 9
    ```

    注意到，特征名被分号分割了，需要将它拼接成字符串：`education_classes_id`，则核心用户有一个名称为`education_classes_id`的特征，它的取值是`feature 9`

  + **ID.featnames**：用户的特征名称和特征取值

    ```txt
    0 birthday;anonymized feature 0
    1 birthday;anonymized feature 1
    2 birthday;anonymized feature 2
    3 birthday;anonymized feature 3
    4 birthday;anonymized feature 4
    5 birthday;anonymized feature 5
    6 birthday;anonymized feature 6
    7 birthday;anonymized feature 7
    8 education;classes;id;anonymized feature 8
    9 education;classes;id;anonymized feature 9
    10 education;classes;id;anonymized feature 10
    11 education;classes;id;anonymized feature 11
    12 education;classes;id;anonymized feature 12
    13 education;concentration;id;anonymized feature 13
    14 education;concentration;id;anonymized feature 14
    15 education;concentration;id;anonymized feature 15
    16 education;concentration;id;anonymized feature 16
    17 education;concentration;id;anonymized feature 17
    18 education;concentration;id;anonymized feature 18
    19 education;concentration;id;anonymized feature 19
    ```

    每一行都是一个**<特征名称，特征取值>**键值对，`anonymized`表示特征值是匿名统计的，这个标识没有用，可以将特征值简化为`feature xx`。

    + 特征名称：有21种，分别是：

      + `birthday`
      + `education_classes_id`
      + `education_concentration_id`
      + `education_degree_id`
      + `education_school_id`
      + `education_type`
      + `education_with_id`
      + `education_year_id`
      + `first_name`
      + `gender`
      + `hometown_id`
      + `languages_id`
      + `last_name`
      + `locale`
      + `location_id`
      + `work_employer_id`
      + `work_end_date`
      + `work_location_id`
      + `work_position_id`
      + `work_start_date`
      + `work_with_id`

      下划线`_`代替了每一行中的分号`;`  这样就构造出属性值名称字段

    + 特征取值：共有224种，`feature 0`~`feature 223`

    每一个特征名称都会对应多个特征取值，因此.egofeat和.feat文件中，每一行需要用224个0/1位来编码用户的属性

  + **ID.feat**：核心用户的朋友的特征

    与.egofeat一样，每一行有224位0/1位用来编码用户的属性，.feat文件中存储的是核心用户的朋友的特征

    ```txt
    1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    2 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 1 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    ```

    上述文本行存储了用户1、2的特征信息。可以看到，每一行得到格式都是 `用户ID 224个0/1位`

+ facebook_combined.txt.gz：它是这10个社交网络聚合出的整体社交网络

本次实验基于1个社交网络进行查询操作，因此只需要从`facebook`文件目录中挑选出其中一个核心用户的社交网络。我挑选的是核心用户为0的社交网络，数据文件的格式为`0.xxx`

在数据集导入之前，需要进行数据整理，方便neo4j的存取。在数据整理之后，需要将数据集导入到neo4j中。使用Python库py2neo，可以实现数据集的整理和导入

---

## py2neo

py2neo是一个用于在Python中与Neo4j图形数据库进行交互的库，使用py2neo可以用Python代码创建、查询和修改图形数据库中的数据。本实验用py2neo创建节点和关系，即将数据集导入到neo4j中

### 连接Neo4j数据库

```python
from py2neo import Graph, Node, Relationship

# 连接到本地的Neo4j数据库
graph = Graph("http://localhost:7474", auth=("neo4j", "12345678"), name="neo4j")
```

+ `http://localhost:7474`是默认的Neo4j HTPP接口地址
+ `auth`参数包含了用户名和密码，`auth = (user, password)``
+ ``name` 参数用于指定连接到的 **数据库名称**。这是在 Neo4j 4.0 及以后版本中引入的多数据库支持功能。

### 节点的建立

节点是图中的实体，节点有以下要素：

+ 标签：节点通过标签来标识其角色，例如 `Person`，表示该节点代表一个人

+ 属性：节点的属性是以==键值对==的形式存储的，例如`name = Jack`，表示节点的 `name` 属性值为 `Jack`

+ 索引和约束：与传统数据库类似，Neo4j 也支持为节点的某些属性创建索引，以及对节点属性应用约束，以提高查询性能和确保数据的完整性和一致性。常见的约束类型包括：

  + **唯一性约束**：确保某个属性的值在数据库中是唯一的
  + **存在性约束**：确保某个节点的特定属性存在
  + **节点标签约束**：确保特定标签的节点符合某些条件

  比如，要确保每个Person节点的email属性是唯一的，并且不能为null，则可以为email属性设置唯一性约束和存在性约束来实现

py2neo创建一个节点：

```python
# 创建节点node1，设置标签为'Person'，并为节点添加属性name、email、age
node1 = Node("Person", name='Dan', email='Dan@163.com', age=19)

# 创建节点node2，设置标签为'Person'，并为节点添加属性name、email、age
node2 = Node('Person', name='Ann', email='Ann@163.com', age=19)

# 将node1、node2节点添加到图数据库中
graph.create(node1)
graph.create(node2)
```

在通过Graph类连接到Neo4j数据库后，可以使用Node类创建两个标签为'Person'的节点，并为节点设置三个属性：name、email、age。以键值对的形式设置属性名称和属性值

### 节点间关系的建立

关系是连接节点之间的结构，通常用于表达节点之间的某种联系。关系有以下的要素：

+ 每一条关系必须连接两个节点，每条关系必须有且只有一个类型。比如`(Person)-[:LOVES]->(Person)` 表示一个人喜欢另一个人，LOVES是这条关系的类型
+ 关系必须是有方向的。A->B和B->A是两个不同的关系，因为它们有不同的方向
+ 关系也可以有属性，这些属性以键值对的形式存储的。比如，可以为LOVES关系添加属性since来表示他们相爱的时间
+ 一个节点可以有多种类型的关系指向其他节点，也可以有多个关系指向同一个节点。比如，一个人可能既是朋友又是同事

使用py2neo创建一条关系：

```python
# 创建LOVES关系，node1(Dan)->node2(Ann)，设置属性since
relationship = Relationship(node1, "LOVES", node2, since=2024)

# 将关系添加到图数据库中
graph.create(relationship)
```

使用Relationship类创建了一条从node1到node2的LOVES关系，并为这个关系添加了属性since=2024，表示他们相爱的时间

![image-20241113223659590](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241113223659590.png)

**注意：**如果建立关系的时候，起始节点或者结束节点不存在，则在建立关系的同时建立这个节点；起始节点可以和结束节点是同一个点，这时候的关系就是这个点指向它自己

### 节点/关系属性值的更新

以更新节点node1的属性email为例，使用graph.push将更新写入到图数据库中：

```python
# 用索引的方式访问属性值，修改node1的email属性值
node1['email'] = "Dnn@gmail.com"

# 更新写入图数据库中
graph.push(node1)
```

本次实验中了解py2neo的这些功能就足够了，进一步的学习（使用Python代码查找节点/关系、Python中运行Cypher语言）可以参照：https://www.cnblogs.com/edkong/p/16167542.html. 节点和关系的介绍可以参照：https://neo4j.com/docs/getting-started/graph-database/

---

## 数据集整理和导入

`lab3.py`实现了数据集的整理和导入

1. **获得属性列表和用户的circle信息**

   + 在创建节点前，需要先读取0.featnames, 0.circles文件，**获得属性列表和每个用户的circle列表**，这部分对应的是`get_feature_info`和`get_circle_info`：

     + `get_feature_info`：读取0.featnames文件，使用集合`feature_name`存储属性的名称，使用DataFrame类型的`feature_data`存储<属性ID，属性名称，属性值>

     + `get_circle_info`：读取0.circles文件，使用字典`user_circle_dict`存储用户ID与用户所属circle的映射

2. **创建中心社交网络图**

   + 调用py2neo.Graph函数，**连接到本地的neo4j数据库**：	

     ```python
     graph = Graph("http://localhost:7474/", auth=("neo4j", "12345678"), name = "neo4j")
     ```

   + 在创建社交网络图时，**首先创建非中心用户的节点和关系**：

     + `create_alter_node`：读取0.feat文件，创建非中心用户的节点。调用函数create_node创建节点，然后将创建好的节点保存在字典node_dict中

     + `create_alter_relationship`：读取0.edge文件，创建非中心用户的关系。0.edge文件每一行都存储了两个具有朋友关系的用户的ID，在node_dict中找到这两个用户对应的节点，然后创建关系

   + **然后创建中心用户的节点和关系**：

     + `create_ego_node`：读取0.egofeat文件，创建中心用户的节点。调用函数create_node创建节点，然后返回创建好的节点

     + `create_ego_relationship`：建立中心点与非中心点之间的关系。遍历node_dict，建立ego_node与非中心用户的节点之间的关系

   + create_node函数根据根据属性编码行line, 属性信息feature_name、feature_data，circle信息user_circle_dict 创建一个节点，函数返回创建好的节点
     + 属性编码行line的格式为：`用户ID 0/1编码···`。当创建中心用户的节点时，需要在中心用户的属性编码行之前加上中心用户的ID，即`ego_id + feature_line``
     + ``feature_data` 中存储的属性索引与 0/1 编码的位一一对应。例如，当第 `i` 位编码为 1 时，表示该用户具有属性 `feature_data.loc[i, "name"]`，其对应的值为 `feature_data.loc[i, "data"]`
     + 有部分用户并没有circle信息，因此属性circle应该设置为：`circle = user_circle_dict.get(user_id, [])`

   + create_eho_graph函数依次调用`create_alter_node`, `create_alter_relationship` , `create_ego_node`, `create_ego_relationship`，**创建中心社交网络图**：

     ```python
     def create_ego_graph(feature_name, feature_data, user_circle_dict):
         # 创建非中心用户的节点和关系
         node_dict = create_alter_node(feature_name, feature_data, user_circle_dict)
         create_alter_relationship(node_dict)
         
         # 创建中心用户的节点和关系
         ego_node = create_ego_node(feature_name, feature_data, user_circle_dict)
         create_ego_relationship(ego_node, node_dict)

打开http://localhost:7474/，在最上方的命令行中输入`MATCH p=()-->() RETURN p LIMIT 25`，即可看到导入的社交网络图的一部分：

![image-20241115103410196](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115103410196.png)

导入的节点共有348个，关系共有5385个

![image-20241115104454198](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115104454198.png)

+ 中心用户（1个）+ 非中心用户（347个） = 节点数量（348个）

+ 在 `.edges` 文件中，每对用户关系都包含两个记录：一条表示 `Node1 -> Node2`，另一条表示 `Node2 -> Node1`，即每一对用户都建立了双向的好友关系，在图中可以看到非中心用户之间都是双向的好友关系。非中心用户之间共有5038个关系
+ 中心用户与非中心用户（347个）一一建立关系，中心用户与非中心用户之间共有347个关系

---

## Neo4j

Neo4j图数据库的原理：

https://www.cnblogs.com/hanease/p/15712866.html

---

## Cypher查询

创建的中心社交网络图：

![image-20241115170632079](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115170632079.png)

1. **检索所有gender属性为77且education;degree;id为20的Person**

   ```cypher
   MATCH (n:Person) 
   WHERE 77 IN n.gender AND 20 IN n.education_degree_id
   RETURN n
   ```

   由于属性是用列表存储，因此要用`IN`来判断节点是否有某个属性值

   ![image-20241115170953681](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115170953681.png)

2. **检索所有gender属性为78且education;degree;id为20或22的Person**

   ```cypher
   MATCH (n:Person) 
   WHERE 78 IN n.gender 
   AND 
   ANY(id IN n.education_degree_id WHERE id IN [20, 22])
   RETURN n
   ```

   ![image-20241115184041241](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115184041241.png)

3. **为Person增设年龄age属性，数值自行设定，可以随机化，要求年介于18岁-30岁之间，尽量分布均匀**

   可以使用`RAND()`函数，`RAND()`函数能生成一个0到1之间的随机浮点值，可以用`toInteger(min + (RAND() * (max - min + 1)))`产生一个[min, max]之间的随机整数值

   ```cypher
   MATCH (p:Person)
   SET p.age = toInteger(18 + (RAND() * (30 - 18 + 1)))
   RETURN p.person_id, p.age 
   ORDER BY p.person_id
   ```

   ![image-20241115185845792](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115185845792.png)

4. **检索每个Person的朋友的数量**

   ```cypher
   MATCH (p:Person)-[:Be_Friend_With]-(friend:Person)
   RETURN p.person_id, COUNT(friend) AS friend_count 
   ORDER BY p.person_id
   ```

   要注意朋友关系是==单向==的，因此要用`(a:Person)-[:Be_Friend_With]-(b:Person)`来计算`a`的朋友数量

   ![image-20241115185753848](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115185753848.png)

5. **检索朋友平均年龄值在25岁以下的Person集合**

   ```cypher
   MATCH (p:Person)-[:Be_Friend_With]-(friend:Person)
   WITH p, AVG(friend.age) AS average_age
   WHERE average_age < 25
   RETURN p.person_id, average_age
   ORDER BY p.person_id
   ```

   `WITH`主要用于在查询的不同阶段之间传递数据，并且通常与聚合函数、筛选条件、子查询等一起使用。

   ![image-20241115190646018](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115190646018.png)

6. **检索年龄最大的前10个Person**

   ```cypher
   MATCH (p:Person) 
   RETURN p.person_id, p.age
   ORDER BY p.age DESC LIMIT 10
   ```

   ![image-20241115190841270](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115190841270.png)

7. **删除所有年龄为18和19的Person**

    ```cypher
    MATCH (p:Person)
    WHERE p.age IN [18, 19]
    DETACH DELETE p
    ```

    DETACH能与p有关的关系也一并删除  

    删除前：

    ![image-20241117232913753](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117232913753.png)

      

    删除后：

    查询年龄为18，19的节点：

    ![image-20241117233154315](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233154315.png)

    删除后按照年龄升序排列节点：

    ![image-20241117233304578](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233304578.png)

      

8. **检索某个Person的所有朋友和这些朋友的所有朋友；**

      ```cypher
      MATCH (p:Person {person_id:10})-[:Be_Friend_With]-(friend:Person)-[:Be_Friend_With]-(fof:Person)
      RETURN p.person_id, friend.person_id, COLLECT(DISTINCT fof.person_id) AS fof_list
      ```

      检索id=10的节点的朋友和朋友的朋友。使用`COLLECT()`函数将朋友的朋友作为一个列表，注意到朋友的朋友可能有重复的情况，因此要使用`DISTINCT`关键字，筛选出不重复的节点加入到列表中

      ![image-20241115193132573](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115193132573.png)

9. **检索某个Person的所有朋友集合和其所在的circle的所有Person集合；**

      ```cypher
      MATCH (p:Person {person_id:10})-[:Be_Friend_With]-(friend:Person)
      WITH p, COLLECT(friend.person_id) AS friend_list
      MATCH (member:Person) WHERE ANY(circle IN member.circle WHERE circle IN p.circle)
      RETURN p.person_id, friend_list, COLLECT(member.person_id) AS circle_list
      ```

      这其实是两个查询，如果要用一个语句实现这两个查询，则需要用WITH传递参数，然后进行两次匹配MATCH。注意circle属性是一个列表，在查找member是否与p有相同的circle时，需要用`ANY(circle IN member.circle WHERE circle IN p.circle) `遍历member.circle的每个circle，并在p.circle中查找是否有共有的circle

      ![image-20241115195122425](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115195122425.png)

10. **任选三对Person，查找每一对Person间的最短关系链（即图模型的最短路）；**

     ```cypher
     MATCH (p1:Person), (p2:Person)
     WHERE p1<>p2	// 选取的p1与p2不相等
     WITH p1, p2
     LIMIT 3			// 选取3对
     MATCH path = shortestPath((p1)-[:Be_Friend_With*]-(p2))
     RETURN p1.person_id, p2.person_id, length(path) AS shortestPathLength
     ```

     使用函数`shortestPath`，在节点p1与p2的关系链`Be_Friend_With`中选出最短的关系链，path就是这个最短的关系链。注意关系使用的是`[:Be_Friend_With*]`，即p1和p2之间可以有任意数量的 `Be_Friend_With` 关系

       ![image-20241115201935782](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115201935782.png)

       显示这条最短路径上的所有节点和关系的信息：

       ```cypher
       MATCH (p1:Person), (p2:Person)
       WHERE p1<>p2
       WITH p1, p2
       LIMIT 3
       MATCH path = shortestPath((p1)-[:Be_Friend_With*]-(p2))
       RETURN p1 AS Person1, p2 AS Person2, length(path) AS shortestPathLength,
       nodes(path), relationships(path)	// 使用nodes和relationships获得最短关系链上的所有节点和关系
       ```

       ![image-20241115202312356](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241115202312356.png)

11. **对于人数少于两个的circle，删除掉这些circle里的Person的表示circle信息的属性；**

    ```Cypher
    MATCH (p:Person)
    UNWIND p.circle AS circle
    WITH circle, COUNT(p) AS count
    WHERE count < 2
    WITH COLLECT(circle) AS remove_circles
    
    MATCH (p:Person)
    SET p.circle = [circle IN p.circle WHERE NOT circle IN remove_circles]
    RETURN p.person_id, p.circle
    ```

      WITH可以实现分组操作：当 `WITH` 中使用了聚合函数（如 `COUNT`、`COLLECT` 等），Cypher 会自动按非聚合字段对数据进行分组。

      ![image-20241117233857613](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233857613.png)

      要删除circle属性元素的节点：

      ![image-20241117121723594](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117121723594.png)

      删除之后：

      ![image-20241117233821753](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233821753.png)

12. **按年龄排序升序所有Person后，再按hometown;id属性的字符串值降序排序，然后返回第5、6、7、8、9、10名Person，由于一些节点的hometown;id可能是空的（即没有这个属性），对于null值的节点要从排序列表里去掉；**

      ```Cypher
      MATCH (p:Person)
      WHERE size(p.hometown_id) > 0
      RETURN p.person_id, p.age, p.hometown_id
      ORDER BY p.age ASC, p.hometown_id DESC
      SKIP 4
      LIMIT 6
      ```

       使用`SKIP 4`可以跳过前4个节点

       ![image-20241117233656338](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233656338.png)

13. **检索某个Person的二级和三级朋友集合（A的直接朋友（即有边接）的称之为一级朋友，A的N级朋友的朋友称之为N+1级朋友，主要通过路径长度来区分，即A的N级朋友与A的所有路径中，有一条长度为N）**

      ```cypher
      MATCH (p:Person {person_id:10})-[:Be_Friend_With*2]-(f2:Person)
      WHERE p <> f2
      WITH p, COLLECT(DISTINCT f2.person_id) AS second_list
      
      MATCH (p)-[:Be_Friend_With*3]-(f3:Person)
      WHERE p <> f3
      WITH second_list, COLLECT(DISTINCT f3.person_id) AS third_list
      RETURN second_list, third_list
      ```

    根据定义，一级朋友和二级朋友可以重合，即只要有一个从A到p的路径长度为N，则p就是A的N级朋友

      > 我认为最好不重合，即从A到p的最短路径为N，则p是A的N级朋友

      如果想一级朋友和二级朋友之间不重合，可以使用如下的语句：

      ```Cypher
      MATCH (p:Person {person_id:10})-[:Be_Friend_With*2]-(f2:Person)
      WHERE p <> f2 AND length(shortestPath((p)-[:Be_Friend_With*]-(f2))) = 2
      RETURN p, COLLECT(DISTINCT f2.person_id)
      ```

      但是在这个图中，由于所有的用户都和中心用户有朋友关系，因此任意两个用户之间的最短路径一定 <= 2，二者可能是直连（最短路径=1）或 通过中心用户连接（最短路径=2），因此如果以最短路径定义N级朋友，则这个图中没有三级朋友

      还需要注意一点：shortestPath需要指定起始和终止的两个节点，即p和f2在shortestPath函数之前就被指定好了，关系为`-[:Be_Friend_With*]-`表示shortestPath会检索它们之间的所有路径去寻找最短的路径

14. **获取某个Person的所有朋友的education;school;id属性的list；**

      ```cypher
      MATCH (p:Person {person_id:10})-[:Be_Friend_With]-(friend:Person)
      WHERE size(friend.education_school_id) > 0
      RETURN COLLECT(DISTINCT friend.education_school_id)
      ```

       查询节点10的所有朋友的education_school_id，过滤其中的空列表，将结果汇聚为列表。注意：如果条件为`WHERE friend.education_school_id IS NOT NULL`，结果中仍然会有空的列表出现，这是因为属性education_school_id是存在的，因此这个属性非空，只是内容为空列表。因此这个条件是不能筛选出空列表的

       ![image-20241117233800016](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117233800016.png)

15. **任选三对Person，查找每一对Person的关系路径中长度小于10的些路径，检索出这些路径上年龄大于22的Person集合，在这一查询中由于数据量及Person的选取问题，可能导致该查询难以计算出结果，因此可以将10这一数字下调至可计算的程度（自行决定，但请保证>=2），或者更换Person对；**

      ```cypher
      MATCH (p1:Person), (p2:Person)
      WHERE p1 <> p2
      WITH p1, p2
      LIMIT 3
      
      MATCH path = (p1)-[:Be_Friend_With*2..5]-(p2)
      UNWIND nodes(path) as node
      WITH p1, p2, COLLECT(DISTINCT
          CASE
              WHEN node.age > 22 THEN node.person_id
              END
      ) AS list
      RETURN p1.person_id, p2.person_id, list 
      ```

       ![image-20241117232605327](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117232605327.png)

       ![image-20241117232540598](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117232540598.png)

       为了方便演示，我将这个条件严苛了一些：寻找最短路径的长度在2~5之间的，且路径上所有元素的年龄都>22的元素集合，最终以路径集合的形式展示出来

       ```cypher
       MATCH path = shortestPath((p1:Person)-[:Be_Friend_With*]-(p2:Person))
       WHERE p1 <> p2 AND length(path) >= 2
       WITH p1, p2
       LIMIT 3
       
       MATCH path = allShortestPaths((p1)-[:Be_Friend_With*..5]-(p2))
       WHERE ALL (node IN nodes(path) WHERE node.age > 22)
       WITH p1, p2, [node IN nodes(path) | node.person_id] AS list
       RETURN p1.person_id, p2.person_id, COLLECT(DISTINCT list) AS persons
       ```

       注意：筛选元素的语法为`[node IN nodes(path) | node.person_id]`，新版的Cypher似乎并不支持`FILTER`了（我使用`FILTER(node IN nodes(Path) WHERE node.age > 22) AS node_list`就会报错）

       ![image-20241117231240124](https://myc-picture.oss-cn-beijing.aliyuncs.com/database/image-20241117231240124.png)

       

















