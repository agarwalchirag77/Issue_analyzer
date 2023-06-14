# def get_source_list(data: dict) -> list:
#     src_list = []
#     for each in data['src_objects']:
#         if each['key_level0'] is not 'null':
#             src_list.append(each['key_level0'] + '.' + each['key_level1'])
#     return src_list


data = {


    "selection": {
        "src_objects": ["MyDomainDiscoverableLogin"]
    }
}
print(','.join(data['selection']['src_objects']))
