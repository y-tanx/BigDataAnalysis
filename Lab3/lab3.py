import pandas as pd
from py2neo import Node, Graph, Relationship

# 连接到本地的neo4j数据库
graph = Graph("http://localhost:7474/", auth=("neo4j", "12345678"), name = "neo4j")

# 定义数据集文件路径
featnames_file_name = "./data/0.featnames"
feat_file_name = "./data/0.feat"
circles_file_name = "./data/0.circles"
edge_file_name = "./data/0.edges"
egofeat_file_name = "./data/0.egofeat"

ego_id = 0      # 中心用户的ID

# 读取0.featnames，存储属性信息
def get_feature_info():
    # 初始化feature_name和feature_data
    feature_name = set()
    feature_data = pd.DataFrame(columns = ["id", "name", "data"])
    
    # 读取0.featnames文件
    with open(featnames_file_name, 'r') as file:
        features = file.readlines()
    
    # 从0.featnames中提取ID、属性名称、属性值
    for line in features:
        contents = line.split(";")
        
        # 提取ID和属性名称
        first = contents[0].split(" ")
        id = int(first[0])  # ID
        name = first[1]     # 属性名称的第一个字段
        
        # 拼接属性名称字段
        for i in range(1, len(contents) - 1):
            name += "_" + contents[i]
        
        # 提取属性值
        last = contents[-1].split(" ")  # anonymized feature xx
        data = int(last[2])
        
        # 创建DataFrame保存ID、属性名、属性值
        temp = pd.DataFrame({"id": [id], "name": [name], "data": [data]})
        
        # 更新feature_data和feature_name
        feature_data = pd.concat([feature_data, temp], ignore_index = True)
        feature_name.add(name)
        
    return feature_name, feature_data

# 读取0.circle文件，存储节点的circle信息
def get_circle_info():
    # 初始化user_circle_dict
    user_circle_dict = dict()
    
    # 读取0.circle文件
    with open(circles_file_name, 'r') as file:
        circles = file.readlines()
        
    # 从0.circle中提取用户ID与用户所属circle的映射
    for line in circles:
        contents = line.split("\t")
        circle_name = contents[0]
        circle_user_list = contents[1:]
        
        # 遍历circle_user_list，向user_circle_dict中添加<用户ID，circle_name>键值对
        for user in circle_user_list:
            circle_user_id = int(user)  # 用户ID
            user_circle_dict.setdefault(circle_user_id, []).append(circle_name)
    
    return user_circle_dict
            
# 创建非中心点的用户的节点
def create_alter_node(feature_name, feature_data, user_circle_dict):
    # 读取0.feat文件
    with open(feat_file_name, 'r') as file:
        alter_features = file.readlines()
    
    # 初始化node_dict，用于保存创建的用户节点
    node_dict = dict()
    
    for line in alter_features:   
        # 获取用户的ID
        user_id = int(line.split(" ")[0])
        
        # 创建用户节点
        node = create_node(line, feature_name, feature_data, user_circle_dict)
        
        # 将node保存到node_id
        node_dict[user_id] = node
    
    return node_dict
        
def create_alter_relationship(node_dict):
    # 读取0.edge文件
    with open(edge_file_name, 'r') as file:
        edges = file.readlines()
    
    # 构造非中心点的用户之间的朋友关系
    for line in edges:
        contents = line.split(" ")
        start = int(contents[0])
        end = int(contents[1])
        
        # 从node_dict中获得对应的节点node，创建关系，类型为Be_Friend_With
        relationship = Relationship(node_dict[start], "Be_Friend_With", node_dict[end])
        relationship["undirected"] = True   # 标记为无向边
        
        # 将关系添加到图数据库中
        graph.create(relationship)

def create_ego_node(feature_name, feature_data, user_circle_dict):
    # 读取0.egofeat文件
    with open(egofeat_file_name, 'r') as file:
        ego_feature = file.readline()
    
    # 在0/1编码前添加中心用户的ID
    ego_line = str(ego_id) + " " + ego_feature
    
    # 创建中心用户的节点
    node = create_node(ego_line, feature_name, feature_data, user_circle_dict)
    
    # 返回中心用户的节点
    return node

def create_ego_relationship(ego_node, node_dict):
    # 遍历node_dict,创建非中心用户和中心用户之间的朋友关系
    for key in node_dict.keys():
        relationship = Relationship(node_dict[key], "Be_Friend_With", ego_node)
        relationship["undirected"] = True
        
        # 将关系添加到图数据库中
        graph.create(relationship)
    
def create_node(line, feature_name, feature_data, user_circle_dict):
    # 初始化字典feature_dict，用于存储当前用户的属性信息
    feature_dict = dict()
    for name in feature_name:
        feature_dict[name] = [] # 由于出现了多值属性，因此用列表存储
    
    # 根据0/1编码，将用户的属性名和属性值信息存储在feature_dict中
    contents = line.split(" ")
    user_id = int(contents[0])  # 用户的id
    bitmap = contents[1:]       # 0/1编码
    for i in range(0, len(bitmap)):
        if bitmap[i] == '1':
            # 加入第i位对应的属性名称和属性值
            feature_dict[feature_data.loc[i, "name"]].append(feature_data.loc[i, "data"])
    
    # 创建用户节点，节点的标签是'Person'，根据feature_dict和user_circle_list设置属性
    node = Node("Person",
                person_id = user_id,
                birthday = feature_dict['birthday'],
                education_classes_id = feature_dict['education_classes_id'],
                education_concentration_id = feature_dict['education_concentration_id'],
                education_degree_id = feature_dict['education_degree_id'],
                education_school_id = feature_dict['education_school_id'],
                education_type = feature_dict['education_type'],
                education_with_id = feature_dict['education_with_id'],
                education_year_id = feature_dict['education_year_id'],
                first_name = feature_dict['first_name'],
                gender = feature_dict['gender'],
                hometown_id = feature_dict['hometown_id'],
                languages_id = feature_dict['languages_id'],
                last_name = feature_dict['last_name'],
                locale = feature_dict['locale'],
                location_id = feature_dict['location_id'],
                work_employer_id = feature_dict['work_employer_id'],
                work_end_date = feature_dict['work_end_date'],
                work_location_id = feature_dict['work_location_id'],
                work_position_id = feature_dict['work_position_id'],
                work_start_date = feature_dict['work_start_date'],
                work_with_id = feature_dict['work_with_id'],
                circle = user_circle_dict.get(user_id, []))
    
    # 将节点添加到图数据库中
    graph.create(node)
    
    return node

def create_ego_graph(feature_name, feature_data, user_circle_dict):
    # 创建非中心用户的节点和关系
    node_dict = create_alter_node(feature_name, feature_data, user_circle_dict)
    create_alter_relationship(node_dict)
    
    
    # 创建中心用户的节点和关系
    ego_node = create_ego_node(feature_name, feature_data, user_circle_dict)
    create_ego_relationship(ego_node, node_dict)

if __name__ == "__main__":
    # 删除上一次创建的图
    graph.run("MATCH (n) DETACH DELETE n")
    
    # 初始化feature_name, feature_data, user_circle_dict
    user_circle_dict = dict()
    
    # 从0.featnames获取属性信息
    feature_name, feature_data = get_feature_info()
    
    # 从0.circles获取circle信息
    user_circle_dict = get_circle_info()
    
    # 创建中心社交网络图
    create_ego_graph(feature_name, feature_data, user_circle_dict)
        
        

