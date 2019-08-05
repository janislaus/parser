#%%
import untangle

class Tender:

    def __init__(self, xml):
        self.data = []
        self.parsed_xml = untangle.parse(xml)
        #parent helps to avoid dublicate column names: in case two columns have
        #the same name --> one of them gets the parents name added to its name
        self.parent = ""

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

                    [self.data.append(["_".join([item._name, key]),
                     item._attributes[attr_key_list[idx]]])
                     for idx, key in enumerate(attr_key_list)]


                # if item has children go further down the "tree"
                if (item.children != []):

                    # sometimes cdata entries are divided by <P> tags. this
                    # deals with those occurences
                    if (item.children[0]._name == "P"):

                        value = ". ".join([item.children[i].cdata
                                        for i in range(len(item.children))])

                        if (item._name in col_list):
                            self.data.append([self.parent + "_" + item._name, value])

                        else:
                            self.data.append([item._name, value])

                    else:
                        self.parent = item._name
                        self.get_data(item.children)

                # collect the key value pairs of leafs
                else:

                    if (item.cdata != ""):

                        if (item._name in col_list):
                            self.data.append([self.parent + "_" + item._name, item.cdata])

                        else:
                            self.data.append([item._name, item.cdata])


        return 1
