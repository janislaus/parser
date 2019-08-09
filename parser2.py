# version of parser, that guarantees unique columns names for each feature

import untangle
import re

class Tender:

    def __init__(self, xml):
        self.xml_name = xml
        self.data = []
        self.data_dict = {}
        self.parsed_xml = untangle.parse(xml)


    def get_data(self, children = None):
        '''
        collects recursively all the key-value tupels from the xml file
        '''
        # initialises obj with root for the first call
        if children == None:
            obj = self.parsed_xml.TED_EXPORT
        else:
             obj = children

        # list of tags that exist for all languages (then we only take englich
        # version)
        block_list = ["ML_TI_DOC", "F02_2014", "URI_DOC", "AA_NAME"]

        # iterate over all tags and add the key value pairs to self.data
        for item in obj:
            col_list = [entry[0] for entry in self.data]

            # add key value pairs from the attributes of xml file to data
            attr_key_list = list(item._attributes.keys())


            # only iterate over tags that are not blocked by block_list
            if ((item._name not in block_list) or
                ((item._name in block_list) and ("EN" in attr_key_list))):

                # add key value pairs for attributes
                if (len(attr_key_list) > 0):


                    for idx, key in enumerate(attr_key_list):

                        if ("_".join([item._name, key]) in col_list):

                            count = 0

                            while(("_".join([item._name, key]) + "#" + str(count+1)) in col_list):
                                count+=1


                            self.data.append(["_".join([item._name, key]) + "#" + str(count+1),
                                 item._attributes[attr_key_list[idx]]])

                        else:

                            self.data.append(["_".join([item._name, key]),
                                 item._attributes[attr_key_list[idx]]])



                # if item has children go further down the "tree"
                if (item.children != []):

                    # sometimes cdata entries are divided by <P> tags. this
                    # deals with those occurences
                    if (item.children[0]._name == "P"):

                        value = ". ".join([item.children[i].cdata
                                        for i in range(len(item.children))])

                        if (item._name in col_list):

                            count = 0

                            while((item._name + "#" + str(count+1)) in col_list):
                                count+=1

                            self.data.append([
                                item._name + "#" + str(count+1), value])

                        else:
                            self.data.append([item._name, value])

                    else:
                        self.get_data(item.children)

                # collect the key value pairs of leafs
                else:

                    if (item.cdata != ""):

                        if (item._name in col_list):
                            count = 0

                            while((item._name + "#" + str(count+1)) in col_list):
                                count+=1

                            self.data.append([
                                item._name + "#" + str(count+1), item.cdata])

                        else:
                            self.data.append([item._name, item.cdata])

        return self

    def agg_data(self):
        """
        - gehe über data entscheide, was ähnlichen columns passieren soll:
          mehrheitsentscheid, summieren, etc
        """
        col_list = [item[0] for item in self.data]
        distinct_col_list = [item[0] for item in self.data if not
                            re.search(r".*#[0-9]{1,3}$", item[0])]


        for item in distinct_col_list:
            self.data_dict.update({item: [element[1] for element in
                        self.data if (re.search(r"^" + re.escape(item)
                         + "#{0,1}[0-9]{0,3}$", element[0]))]})


        return self


    def maj_vote(self, columns):

        for col in columns:
            lst = self.data_dict[col]
            self.data_dict.update({col: max(set(lst), key=lst.count)})

        return self

    def sum_items(self, columns):

        for col in columns:
            lst = list(map(float, self.data_dict[col]))
            self.data_dict.update({col: sum(lst)})

        return self
