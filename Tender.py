# version of parser, that guarantees unique columns names for each feature

import untangle
import re

class XmlParser:
    # schon fertig initialieren

    def __init__(self, multi_languague_columns=["ML_TI_DOC", "F02_2014",
                "URI_DOC", "AA_NAME"], columns_of_interest=[]):
        self.multi_languague_columns = multi_languague_columns
        self.columns_of_interest = columns_of_interest

    def find_word(self, dir_xml, word="konsens"):
        parsed_xml = untangle.parse(dir_xml)

        root_node = parsed_xml.TED_EXPORT
        lst = []

        def f(nodes=None):

            for item in nodes:
                if (word in item._name.lower()) or (word in item.cdata.lower()):
                    lst.append(xml)

                # add key value pairs from the attributes of xml file to data
                attr_key_list = list(item._attributes.keys())

                    # add key value pairs for attributes
                if attr_key_list:
                    for attr in attr_key_list:
                        if (word in attr.lower()) or (word in (item._attributes[attr]).lower()):
                            lst.append(xml)

                # if item has children go further down the tree
                if (item.children != []):
                        f(nodes=item.children)

        f(nodes=root_node)
        return lst



    def parse_data(self, dir_xml):
        '''
        collects recursively all the key-value tupels from the xml file, and
        stores them in self.data
        '''
        parsed_xml = untangle.parse(dir_xml)

        root_node = parsed_xml.TED_EXPORT
        lst = []

        def traverse_nodes(nodes=None):
            # list of nodes that exists for all languages (then we only take english
            # version)
            multi_languague_columns = self.multi_languague_columns

            # iterate over all nodes and add the key value pairs to lst
            for item in nodes:
                col_list = [entry[0] for entry in lst]

                # add key value pairs from the attributes of xml file to data
                attr_key_list = list(item._attributes.keys())

                # only iterate over nodes that are not blocked by block_list
                if ((item._name not in multi_languague_columns) or
                    ((item._name in multi_languague_columns) and ("EN" in attr_key_list))):

                    # add key value pairs for attributes
                    if attr_key_list: self.__add_attribute(lst, item, attr_key_list, col_list)

                    # if item has children go further down the tree
                    if (item.children != []):

                        # sometimes cdata entries are divided by <P> tags. this
                        # deals with those occurences
                        if (item.children[0]._name == "P"): self.__add_paragraphed_text(lst, item, col_list)

                        else:
                            traverse_nodes(nodes=item.children)

                    # collect the key value pairs of leafs
                    else: self.__add_leafs(lst, item, col_list)

        traverse_nodes(nodes=root_node)

        return Tender(data=self.__agg_data(lst))

    def __add_attribute(self, lst, item, attr_key_list, col_list):
        """
        adds attribute-key-value pairs for nodes with attributes
        """

        for idx, key in enumerate(attr_key_list):

            if ("_".join([item._name, key]) in col_list):

                count = 0

                while(("_".join([item._name, key]) + "#" + str(count+1)) in col_list):
                    count+=1


                lst.append(["_".join([item._name, key]) + "#" + str(count+1),
                     item._attributes[attr_key_list[idx]]])

            else:

                lst.append(["_".join([item._name, key]),
                     item._attributes[attr_key_list[idx]]])

    def __add_paragraphed_text(self, lst, item, col_list):
        """
        adds key-value-pairs to lst, in case text is paragraphed with <p> tags
        """

        value = ". ".join([item.children[i].cdata
                        for i in range(len(item.children))])

        if (item._name in col_list):

            count = 0

            while((item._name + "#" + str(count+1)) in col_list):
                count+=1

            lst.append([
                item._name + "#" + str(count+1), value])

        else:
            lst.append([item._name, value])

    def __add_leafs(self, lst, item, col_list):
        """
        adds key-value pairs of leafs to lst
        """

        if (item.cdata != ""):

            if (item._name in col_list):
                count = 0

                while((item._name + "#" + str(count+1)) in col_list):
                    count+=1

                lst.append([
                    item._name + "#" + str(count+1), item.cdata])

            else:
                lst.append([item._name, item.cdata])

    def __agg_data(self, lst):
        """
        aggregatest lst (which is a list of list) into a dict. Column names that
        are only differentiated by number are now the same key, while all there
        values get stored as a list for that key.
        Example: [["a#1", val1], ["a#2", val2]] --> {"a": [val1, val2]}
        """
        dct = {}
        columns_of_interest = self.columns_of_interest
        if columns_of_interest:
            distinct_col_list = columns_of_interest

        else:
            col_list = [item[0] for item in lst]
            distinct_col_list = [item[0] for item in lst if not
                                re.search(r".*#[0-9]{1,3}$", item[0])]


        for item in distinct_col_list:
            if item in [element[0] for element in lst]:
                dct.update({item: [element[1] for element in
                            lst if (re.search(r"^" + re.escape(item)
                             + "#{0,1}[0-9]{0,3}$", element[0]))]})

        return dct

class Tender:

    def __init__(self, data):  # : dict
        self.data = data

    def maj_vote(self, columns):

        for col in columns:
            if col in list(self.data.keys()):
                list_values = self.data[col]
                if list_values:
                    self.data.update({col: [max(set(list_values), key=list_values.count)]})

        return self

    def sum_list(self, columns):

        for col in columns:
            if col in list(self.data.keys()):
                no_whitespace_list = [re.sub("[^0-9.,]","",item).replace(",",".") for item in self.data[col]]
                lst = list(map(float, no_whitespace_list))
                self.data.update({col: [sum(lst)]})

        return self

if __name__ == "__main__":
    from progress.bar import Bar
    xml_arr = get_files(DIR_DATA)
    bar = Bar('Processing', max=len(xml_arr))
    a = XmlParser()
    findings = []
    for xml in xml_arr:
        findings.append(a.find_word(xml))
        bar.next()
    print(findings)
