#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
QueryPubMed: A Python API for the NCBI PubMed
Author: https://github.com/liqiming-whu
'''
import os
import re
from datetime import date
import pandas as pd
from Bio import Entrez, Medline


Entrez.email = "liqiming1914658215@gmail.com"                                      
Entrez.api_key = "c80ce212c7179f0bbfbd88495a91dd356708"


class QueryPubMed:
    __slots__ = ['keywords', 'mesh_topic', 'journal', 'year', 'from_date', 'to_date', 'retmax', 'count', 'idlist']
    def __init__(self, keywords=None, mesh_topic=None, journal=None, year=None, from_date=None, to_date=date.today().strftime("%Y/%m/%d"), retmax=1000000):
        self.keywords = keywords
        self.mesh_topic = mesh_topic
        self.journal = journal
        self.year = year
        self.from_date = from_date
        self.to_date = to_date
        assert any(getattr(self, i) for i in self.__slots__[:5]), "At least one parameter is required."
        self.retmax = retmax
        self.count = self.get_count()
        self.idlist = self.search()
        print(self)

    def __repr__(self):
        return f"Search '{self.query}', get {self.count} results."

    __str__ = __repr__

    @property
    def query(self):
        query_list = []
        if self.keywords:
            query_list.append(self.keywords)
        if self.mesh_topic:
            query_list.append(f"{self.mesh_topic}[MeSH Major Topic]")
        if self.journal:
            query_list.append(f"{self.journal}[ta]")
        if self.from_date:
            assert re.compile("\d{4}\/\d{2}\/\d{2}").match(self.from_date), "Date error, fromat: YYYY/MM/DD"
            assert re.compile("\d{4}\/\d{2}\/\d{2}").match(self.to_date), "Date error, fromat: YYYY/MM/DD"
            self.year = None
            query_list.append(f"{self.from_date}: {self.to_date}[dp]")
        if self.year:
            query_list.append(f"{self.year}[dp]")
        
        return " AND ".join(query_list)

    def get_count(self):
        handle = Entrez.egquery(term=self.query)
        record = Entrez.read(handle)
        for row in record["eGQueryResult"]:
            if row["DbName"] == "pubmed":
                count = row["Count"]
        return count

    def search(self):                                                           
       handle = Entrez.esearch(db="pubmed", term=self.query, retmax=self.retmax)
       record = Entrez.read(handle)                                            
       return record["IdList"]

    
    @classmethod
    def get_detail(cls, idlist):
        id_count = len(idlist)
        idlists = [idlist[i:i+10000] for i in range(0,id_count, 10000)] if id_count > 10000 else [idlist]

        pmid_set = []
        title_set = []
        abs_set = []
        au_set = []
        jour_set = []
        date_set = []
        so_set = []
        url_set = []
        for ids in idlists:
            handle = Entrez.efetch(db="pubmed", id=ids, rettype="medline", retmode="text")
            records = Medline.parse(handle)
            for record in records:
                pmid = record.get("PMID", "?")
                print(f"Download {pmid}")
                title = record.get("TI", "?")
                abstract = record.get("AB", "?")
                authors = ", ".join(record.get("AU", "?"))
                journal = record.get("TA", "?")
                pub_date = record.get("DP", "?")
                source = record.get("SO", "?")
                url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}"
                pmid_set.append(pmid)
                title_set.append(title)
                abs_set.append(abstract)
                au_set.append(authors)
                jour_set.append(journal)
                date_set.append(pub_date)
                so_set.append(source)
                url_set.append(url)
        return pmid_set, title_set, abs_set, au_set, jour_set, date_set, so_set, url_set
    
    @classmethod
    def download_detail(cls, idlist, path, saveinfo=None):
        data = {}
        pmid_set, title_set, abs_set, au_set, jour_set, date_set, so_set, url_set = cls.get_detail(idlist)
        if not saveinfo:
            data = {
                "PubMed_ID": pmid_set,
                "Title": title_set,
                "Abstract": abs_set,
                "Authors": au_set,
                "Journal": jour_set,
                "Pub_date": date_set,
                "Source": so_set,
                "HTTP_Link": url_set
            }
        else:
            if "PubMed_ID" in saveinfo:
                data["PubMed_ID"] = pmid_set
            if "Title" in saveinfo:
                data["Title"] = title_set
            if "Abstract" in saveinfo:
                data["Abstract"] = abs_set
            if "Authors" in saveinfo:
                data["Authors"] = au_set
            if "Journal" in saveinfo:
                data["Journal"] = jour_set
            if "Pub_date" in saveinfo:
                data["Pub_date"] = date_set
            if "Source" in saveinfo:
                data["Source"] = so_set
            if "HTTP_link" in saveinfo:
                data["HTTP_link"] = url_set

            assert data, 'Please input part of ("PubMed_ID","Title","Abstract","Authors","Journal","Pub_date","Source","HTTP_Link")'
        df = pd.DataFrame(data)
   
        if path.endswith(".tsv"):
            df.to_csv(path, sep="\t")
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            df.to_excel(path)
        else:
            df.to_csv(path)

    def download_results(self, path, saveinfo=None):
        self.download_detail(self.idlist, path, saveinfo)