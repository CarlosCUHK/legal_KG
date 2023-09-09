import numpy as np
import os
import pandas as pd
import re
import random
import json
import argparse
from tqdm import tqdm
from neo4j import GraphDatabase
import sys

class KGConstructor:
    def __init__(self, file_path, neo4j_uri, neo4j_username, neo4j_password) -> None:
        self.file_path = file_path
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))

    def preprocessing(self, text):
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'^，', '', text)
        return text

    def clear_all_info(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        self.driver.close()
        print("The knowledge graph has been cleared!")

    def create_QA_graph(self, tx, question, answer):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (q:Question {text: $question_text}) "
            "MERGE (a:Answer {text: $answer_text}) "
            "MERGE (q)-[:ANSWER]->(a)"
            "RETURN id(a) AS answer_node_id"
        )
        result = tx.run(query, question_text=question, answer_text=answer)
        return result.single()["answer_node_id"]

    def connect_answer_law(self, session, answer_id, law_id):
        query = f"""
        MATCH (node1), (node2)
        WHERE ID(node1) = {answer_id} AND ID(node2) = {law_id}
        CREATE (node1)-[:REFER_TO_LEGAL_DOCUMENT]->(node2)
        """
        session.run(query)

    def connect_chapter_section(self, tx, parent, child, chapter_num, section_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (c: Chapter {text: $parent_text, chapter: $parent_chapter_number}) "
            "MERGE (s: Section {text: $child_text,  chapter: $child_chapter_number, section: $child_section_number}) "
            "MERGE (c)-[:CONTAIN]->(s)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number = chapter_num, child_chapter_number = chapter_num, child_section_number = section_num)

    def connect_section_point(self, tx, parent, child, chapter_num, section_num, point_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (s:Section {text: $parent_text, chapter: $parent_chapter_number, section: $parent_section_number}) "
            "MERGE (p:Point {text: $child_text, chapter: $child_chapter_number, section: $child_section_number, point: $child_point_number}) "
            "MERGE (s)-[:CONTAIN]->(p)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number=chapter_num, parent_section_number=section_num, child_chapter_number=chapter_num, child_section_number=section_num, child_point_number = point_num)

    def connect_point_subpoint(self, tx, parent, child, chapter_num, section_num, point_num, subpoint_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (p:Point {text: $parent_text, chapter: $parent_chapter_number, section: $parent_section_number, point: $parent_point_number}) "
            "MERGE (sp:Subpoint {text: $child_text, chapter: $child_chapter_number, section: $child_section_number, point: $child_point_number, subpoint: $child_subpoint_number}) "
            "MERGE (p)-[:CONTAIN]->(sp)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number=chapter_num, parent_section_number=section_num, parent_point_number = point_num, child_chapter_number=chapter_num, child_section_number=section_num, child_point_number=point_num, child_subpoint_number=subpoint_num)

    def connect_Chapter_definition(self, tx, parent, child, chapter_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (c: Chapter{text: $parent_text, chapter: $parent_chapter_number}) "
            "MERGE (d: Definition {text: $child_text, chapter: $child_chapter_number}) "
            "MERGE (c)-[:DEFINITION]->(d)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number=chapter_num, child_chapter_number=chapter_num)
    
    def connect_Section_definition(self, tx, parent, child, chapter_num, section_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (s: Section{text: $parent_text, chapter: $parent_chapter_number, section: $parent_section_number}) "
            "MERGE (d: Definition {text: $child_text, chapter: $child_chapter_number, section: $child_section_number}) "
            "MERGE (s)-[:DEFINITION]->(d)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number=chapter_num, child_chapter_number=chapter_num, parent_section_number=section_num, child_section_number= section_num)

    def connect_Point_definition(self, tx, parent, child, chapter_num, section_num, point_num):
        # Create a question node with the 'question' label/tag
        query = (
            "MERGE (p: Point{text: $parent_text, chapter: $parent_chapter_number, section: $parent_section_number, point: $parent_point_number}) "
            "MERGE (d: Definition {text: $child_text, chapter: $parent_chapter_number, section: $parent_section_number, point: $parent_point_number}) "
            "MERGE (p)-[:DEFINITION]->(d)"
        )
        tx.run(query, parent_text=parent, child_text=child, parent_chapter_number=chapter_num, child_chapter_number=chapter_num, parent_section_number=section_num, child_section_number= section_num, parent_point_number=point_num, child_point_number=point_num)

    # 這個函數用來導入QA pair 
    def QAPair2KG(self):
        df = pd.read_excel(self.file_path)
        df = df[['L', 'Q', 'A']]
        for index, row in df.iterrows():
            question = row[1]
            answer = row[2]    
            with self.driver.session() as session:
                answer_node_id = session.execute_write(self.create_QA_graph, question, answer)
            contents = row[0].split(":")
            pattern = r"'.+'的定義"
            match = re.search(pattern, contents[0])
            if match:
                definition = contents[0].split("'")
                definition = definition[1]
                with self.driver.session() as session:
                    # Execute the Cypher query
                    result = session.run("MATCH (n:Definition) RETURN id(n) AS node_id, n.text AS text")
                    for record in result:
                        definition_node_id = record["node_id"]
                        text = record["text"]
                        text = text.split(":")
                        if definition == text[0]:
                            self.connect_answer_law(session, answer_node_id, definition_node_id)
            else:
                if contents[1].lstrip()[0] == '(':
                    subpoint_match = re.search(r'\((.*?)\)', contents[1])
                    if subpoint_match:
                        subpoint = subpoint_match.group(1)
                    point = contents[0].split('.')
                    point = point[0]
                    subpoint = '(' + subpoint + ')'
                    with self.driver.session() as session:
                    # Execute the Cypher query
                        query_info = "MATCH (n:Subpoint {chapter: '57', point: '" + point + "', subpoint: '" + subpoint + "'}) RETURN id(n) AS node_id"
                        result = session.run(query_info)
                        for record in result:
                            subpoint_node_id = record["node_id"]        
                            self.connect_answer_law(session, answer_node_id, subpoint_node_id)                                
                else:
                    point = contents[0].split('.')
                    point = point[0]
                    with self.driver.session() as session:
                    # Execute the Cypher query
                        query_info = "MATCH (n:Point {chapter: '57', point: '" + point + "'}) RETURN id(n) AS node_id"
                        result = session.run(query_info)
                        for record in result:
                            point_node_id = record["node_id"]
                            self.connect_answer_law(session, answer_node_id, point_node_id)   
                                             
    # 這個函數用來導入法律文檔
    def Legal2KG(self):
        with open(self.file_path, 'r', encoding='utf-8') as my_file:
            for line in my_file:
                data = json.loads(line)
        title = data["title"]
        with self.driver.session() as session:
            for section, value in data.items():
                ref_chapter_pattern = r'第.*?章'
                substrings = re.findall(ref_chapter_pattern, title)
                chapter_num = substrings[0][1:-1]
                if section != "main_definition" and section != "title":
                
                    ref_section_pattern = r'第.*?部'
                    substrings = re.findall(ref_section_pattern, section + ": "+ value["chapter_name"])
                    section_num = substrings[0][1:-1]
                    session.execute_write(self.connect_chapter_section, title, section + ": "+ value["chapter_name"], chapter_num, section_num)
                    for subsection, value1 in value.items():
                        if subsection != "chapter_name":
                            if value1["content"]:
                                
                                if value1["heading"] != "釋義":
                                    ref_point_pattern = r': .*?\.'
                                    substrings = re.findall(ref_point_pattern, value["chapter_name"] + ": " + subsection + value1["heading"])
                                    if len(substrings) > 0:
                                        point_num = substrings[0][1:-1]
                                        point_num = point_num.replace(" ", "")
                                        point_num = point_num.replace("*", "")
                                        session.execute_write(self.connect_section_point, section + ": "+ value["chapter_name"], value["chapter_name"] + ": " + subsection + value1["heading"], chapter_num, section_num, point_num)
                                for point, value2 in value1["content"].items():
                                    if value1["heading"] != "釋義":
                                        
                                        if not isinstance(value2, dict): 
                                            ref_subpoint_pattern = r'\([0-9a-zA-Z]+\)'
                                            substrings = re.findall(ref_subpoint_pattern, self.preprocessing(value2))
                                            if re.match(ref_subpoint_pattern, self.preprocessing(value2)):
                                                subpoint_num = substrings[0]
                                            else:
                                                subpoint_num = "content"
                                           
                                            session.execute_write(self.connect_point_subpoint, value["chapter_name"] + ": " + subsection + value1["heading"], self.preprocessing(value2), chapter_num, section_num, point_num, subpoint_num)
                                        else:
                                            for def_name, def_content in value2.items():
                                                session.execute_write(self.connect_Point_definition, value["chapter_name"] + ": " + subsection + value1["heading"], self.preprocessing(def_name) + ": " + self.preprocessing(def_content), chapter_num, section_num, point_num)
                                    else:
                                        if isinstance(value2, dict):
                                            for def_name, def_content in value2.items():
                                                session.execute_write(self.connect_Section_definition, section + ": "+ value["chapter_name"], self.preprocessing(def_name) + ": " + self.preprocessing(def_content), chapter_num, section_num)
                                        else:
                                            session.execute_write(self.connect_Section_definition, section + ": "+ value["chapter_name"], self.preprocessing(value2), chapter_num, section_num)  
                elif section == "main_definition":
                    for def_name, def_content in value.items():
                        session.execute_write(self.connect_Chapter_definition, title, self.preprocessing(def_name) + ": "+ self.preprocessing(def_content), chapter_num)


    def connect_two_nodes(self, session, start, end):
        query = f"""
        MATCH (node1), (node2)
        WHERE ID(node1) = {start} AND ID(node2) = {end}
        CREATE (node1)-[:REFER_TO]->(node2)
        """
        session.run(query)
        
    # 這個函數用來連接互相引用的法律條文        
    def connect_law_subtask(self, session, node_id, text, chapter_num):
        ref_point_pattern = r'第[0-9()a-zA-Z或，及、]+條'
        substrings = re.findall(ref_point_pattern, text)

        for substring in substrings:
            substring = substring[1:-1]
            pattern = r'[及或，、]'
            each_points = re.split(pattern, substring)
            parts = each_points[0].split("(")
            point_num = parts[0]
            for each_point in each_points:
                match = re.search(r'\([^)]*\)', each_point)                  
                if not match:
                    local_result = session.run("MATCH (node:Point {chapter: \""+ chapter_num + "\"" +", point: \"" + each_point + "\"" +"})RETURN id(node) AS node_id")
                    for local_record in local_result:
                        connected_node_id = local_record["node_id"] 
                        self.connect_two_nodes(session, node_id,connected_node_id)
                else:
                    local_pattern = r'(\d+[A-Z]*)\((\d+[A-Z]*)\)'
                    
                    if each_point.startswith("(") and each_point.endswith(")"):
                        each_point = str(point_num) + each_point
                    parts = each_point.split("(")
                    each_point = parts[0] + "(" + parts[1]
                    
                    local_match = re.search(local_pattern, each_point)
                    if local_match:
                        point_num = local_match.group(1)
                        subpoint_num = local_match.group(2)
                        local_result = session.run("MATCH (node:Subpoint {chapter: \""+ chapter_num + "\"" +", point: \"" + point_num + "\"" + ", subpoint: \""+ subpoint_num + "\""  +"})RETURN id(node) AS node_id")
                        for local_record in local_result:
                            connected_node_id = local_record["node_id"] 
                            self.connect_two_nodes(session, node_id, connected_node_id)
                    else:
                        local_result = session.run("MATCH (node:Point {chapter: \""+ chapter_num + "\"" +", point: \"" + each_point.split("(")[0] + "\"" +"})RETURN id(node) AS node_id")
                        for local_record in local_result:
                            connected_node_id = local_record["node_id"] 
                            self.connect_two_nodes(session, node_id, connected_node_id)

        ref_subpoint_pattern = r'第[0-9()a-zA-Z或，及]+款'
        substrings = re.findall(ref_subpoint_pattern, text)
        for substring in substrings:
            substring = substring[1:-1]
            pattern = r'[及或，]'                
            query = f"""
            MATCH (node)
            WHERE ID(node) = {node_id}
            RETURN node.point AS point
            """
            my_points = session.run(query)
            for tmp_point in my_points:
                my_point = tmp_point["point"]
            each_subpoints = re.split(pattern, substring)
            for each_subpoint in each_subpoints: 
                parts = each_subpoint.split(")")
                each_subpoint = parts[0] + ")"
                if my_point:
                    local_result = session.run("MATCH (node:Subpoint {chapter: \""+ chapter_num + "\"" +", point: \"" + my_point + "\"" + ", subpoint: \""+ each_subpoint + "\""  +"})RETURN id(node) AS node_id")
                    for local_record in local_result:
                        connected_node_id = local_record["node_id"] 
                        self.connect_two_nodes(session, node_id, connected_node_id)

    def connect_related_law(self):
        with self.driver.session() as session:
            # Execute the Cypher query
            #result = session.run("MATCH (n) RETURN id(n) AS node_id, n.text AS text, n.chapter AS chapter, n.point AS point")
            result = session.run("MATCH (n) RETURN id(n) AS node_id, n.text AS text, n.chapter AS chapter")
            for record in result:
                node_id = record["node_id"]
                text = record["text"]
                chapter_num = record["chapter"]

                pattern = r'\(第[0-9()a-zA-Z]+章\)第[0-9()a-zA-Z]+條'
                substrings = re.findall(pattern, text)
                for substring in substrings:
                    local_pattern = r'第([^章]+)章'
                    local_match = re.search(local_pattern, substring)
                    if local_match:
                        extracted_value = local_match.group(1)
                        local_chapter_num = extracted_value
                    self.connect_law_subtask(session, node_id, text, local_chapter_num)
                    text = re.sub(pattern, '', text)
                self.connect_law_subtask(session, node_id, text, chapter_num)

def parse_args():
    parser = argparse.ArgumentParser("", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # 讀取extracted的json文档可以将legal document放入KG中
    # 讀取QA pair的xlsx文檔可以将問題答案和法律文檔連接起來
    parser.add_argument("--file_format", type=str, default="xlsx", help="determine what kind of files to be read")
    parser.add_argument("--uri", type=str, default="bolt://127.0.0.1:7687",
        help="neo4j uri")
    parser.add_argument("--username", type=str, default="neo4j",
        help="neo4j username")
    parser.add_argument("--password", type=str, default="12345678",
        help="neo4j password")
    parser.add_argument("--filepath", type=str, default="generated_gpt4_full.xlsx",
        help="path of input file")
    parser.add_argument("--clear_KG", default=False, help="clear all the nodes in KG")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    kgconstructor = KGConstructor(args.filepath, args.uri, args.username, args.password)
    if args.clear_KG:
        kgconstructor.clear_all_info()
    if args.file_format == "xlsx":
        kgconstructor.QAPair2KG()
    if args.file_format == "json":
        kgconstructor.Legal2KG()
        kgconstructor.connect_related_law()
    
