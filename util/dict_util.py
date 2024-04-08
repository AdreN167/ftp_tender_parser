import numpy as np

class KeyValue():
    def __init__(self, key, value, valueB = {}):
        self.key = key
        self.value = value
        self.valueB = valueB

class DictUtil():

    @staticmethod
    def merging_dictionaries(dictA, dictB):
        listA = [KeyValue(keyA, valueA) for keyA, valueA in dictA.items()]
        listB = [KeyValue(keyB, valueB) for keyB, valueB in dictB.items()]

        for item in listB:
            if item.key not in (item.key for item in listA):
                listA.append(item)
            elif isinstance(item.value, dict) and isinstance(next((i for i in listA if i.key == item.key), None).value, dict):
                listA.append(KeyValue(item.key, item.value, next((i for i in listA if i.key == item.key), None).value))

        result = {}
        for item in listA:
            if isinstance(item.value, dict):
                result[item.key] = DictUtil.merging_dictionaries(item.value, item.valueB)
            else:
                result[item.key] = item.value

        return result

    @staticmethod
    def dictionary_compression(item_dict, is_dict = False, keys = np.array([], dtype=str), del_keys = np.array([], dtype=str), level0_keys = np.array([], dtype=str), result_dict = {}, level = 0):
      for key, value in item_dict.items():
          if key not in result_dict:
              result_dict[key] = ""
          if level == 0:
              level0_keys = np.append(level0_keys, key)
          if isinstance(value, dict):
              keys = np.append(keys, key)
              DictUtil.dictionary_compression(value, is_dict = True, keys = keys, del_keys = del_keys, level0_keys = level0_keys, result_dict = result_dict, level = level + 1)
              try:
                  del result_dict[key]
              except:
                  ...
              keys = np.delete(keys, np.where(keys == key))
          else:
              if is_dict:
                str_key = ""
                for item in keys:#
                  str_key += item + "_"
                result_dict[(str_key + key)] = value
                del_keys = np.append(del_keys, key)
              else:
                result_dict[key] = value

      difference_del = np.setdiff1d(del_keys, level0_keys)
      for item in difference_del: #
          try:
              del result_dict[item]
          except KeyError:
              continue

      return result_dict