#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
QueryRCSB: A Python API for the RCSB Protein Data Bank
Author: https://github.com/liqiming-whu
'''
import os
import requests
import json
import warnings


class QueryRCSB:
    headers = {'Content-Type': 'application/json'}
    search_api = "https://search.rcsb.org/rcsbsearch/v1/query"
    download_api = "https://files.rcsb.org/download/"

    def __init__(self, text=None, species=None, molecule=None, method=None, retmax=1000):
        self.text = text
        self.species = species
        self.molecule = molecule
        self.method = method
        self.retmax = retmax

        if method:
            assert method in ("ELECTRON MICROSCOPY", "X-RAY DIFFRACTION", "SOLUTION NMR"),\
                'Input one method in ("ELECTRON MICROSCOPY", "X-RAY DIFFRACTION", "SOLUTION NMR")'

    def __repr__(self):
        return f"Query(text={self.text},species={self.species},molecule={self.molecule},method={self.method})"

    __str__ = __repr__

    def results(self):
        data = self.query_data()
        assert data, "At least one query item is required."
        response = requests.post(url=self.search_api, headers=self.headers, data=json.dumps(data))

        if response.status_code == 200:
            pass
        else:
            warnings.warn("Retrieval failed, returning None")
            return None

        results_data = json.loads(response.content)
        results_count = results_data["total_count"]
        print(f"{self}, get {results_count} results.")
        pdb_idlist = [i['identifier'] for i in results_data['result_set']]

        return pdb_idlist

    def query_data(self):
        query_nodes = []
        data = {}

        if self.text:
            query_nodes.append(self.text_data)
        if self.species:
            query_nodes.append(self.species_data)
        if self.molecule:
            query_nodes.append(self.molecule_data)
        if self.method:
            query_nodes.append(self.method_data)

        if query_nodes:
            data = {
                "query": {
                    "type": "group",
                    "logical_operator": "and",
                    "nodes": query_nodes
                },
                "return_type": "entry",
                "request_options": {
                    "pager": {
                        "start": 0,
                        "rows": self.retmax
                    },
                    "scoring_strategy": "combined",
                    "sort": [
                        {
                            "sort_by": "score",
                            "direction": "desc"
                        }
                    ]
                }
            }
        return data

    @property
    def text_data(self):
        data = {
            "type": "terminal",
            "service": "text",
            "parameters": {
                "value": self.text
            }
        }

        return data

    @property
    def species_data(self):
        data = {
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entity_source_organism.ncbi_scientific_name",
                "operator": "exact_match",
                "value": self.species
            }
        }

        return data

    @property
    def molecule_data(self):
        data = {
            "type": "terminal",
            "service": "text",
            "parameters": {
              "attribute": "entity_poly.rcsb_entity_polymer_type",
              "operator": "exact_match",
              "value": self.molecule
            }
        }

        return data

    @property
    def method_data(self):
        data = {
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "exptl.method",
                "operator": "exact_match",
                "value": self.method
            }
        }

        return data

    @classmethod
    def get_pdb_content(cls, pdb_id, filetype='pdb', compression=False):
        full_url = cls.download_api + pdb_id + "." + filetype
        if compression:
            full_url += ".gz"
        else:
            pass

        response = requests.get(full_url)
        if response.status_code == 200:
            pass
        else:
            warnings.warn("Retrieval failed, returning None")
            return None

        if compression:
            result = response.content
        else:
            result = response.text

        return result

    @classmethod
    def download(cls, pdb_id, filename, filetype='pdb'):
        print(f"Start downloading file {pdb_id}.{filetype}...")
        compression = True if filename.endswith(".gz") else False
        flag = 0
        while flag < 3:
            content = cls.get_pdb_content(pdb_id, filetype, compression)
            flag += 1
            if content:
                break
            else:
                print(f"{pdb_id}.{filetype} download failed. Retrying...")
        if not content:
            return None
        if compression:
            with open(filename, "wb") as g:
                g.write(content)
        else:
            with open(filename, "w") as f:
                f.write(content)
        return True

    def download_results(self, dirpath):
        if not os.path.exists(dirpath):
            os.mkdir(dirpath)
        idlist = self.results()
        for pdb in idlist:
            filename1 = os.path.join(dirpath, f"{pdb}.pdb")
            filename2 = os.path.join(dirpath, f"{pdb}.cif")
            if not (os.path.exists(filename1) or os.path.exists(filename2)):
                if self.download(pdb, filename1):
                    print(f"{pdb}.pdb download completed.")
                elif self.download(pdb, filename2, filetype="cif"):
                    print(f"{pdb}.cif download completed.")
                else:
                    print(f"{pdb} download failed.")
            else:
                print(f"{pdb} download completed.")
