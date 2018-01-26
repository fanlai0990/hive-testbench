# -*- coding: utf-8 -*-
from sql_operator import ROOT


# DAG base classes
class DAGNode(object):
    # VALID TYPE includes all possible type for a node, this is set to BASE here
    # for debugging
    VALID_TYPES = {ROOT}

    def __init__(self, type, name, fanout=1, in_stream=None, out_stream=None,
                 soft_factor=None):
        # private variables
        self.__fanout = fanout
        # public variables
        self._type = type
        self.__valid_type()
        self._name = name
        self._sub_nodes = []
        self._prev_nodes = []
        self._in_stream = in_stream
        self._out_stream = out_stream

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, val):
        self._type = val

    @property
    def name(self):
        return self._name

    @property
    def sub_nodes(self):
        assert self._sub_nodes is not None, "node:%s sub node is None" \
                                            % self._name
        return self._sub_nodes

    @sub_nodes.setter
    def sub_nodes(self, vals):
        self._sub_nodes = vals

    @property
    def prev_nodes(self):
        assert self._prev_nodes is not None, "node:%s prev node is None" \
                                             % self._name
        return self._prev_nodes

    @property
    def in_stream(self):
        return self._in_stream

    @in_stream.setter
    def in_stream(self, val):
        self._in_stream = val

    @property
    def out_stream(self):
        return self._out_stream

    @out_stream.setter
    def out_stream(self, val):
        self._out_stream = val

    @property
    def fanout(self):
        return self.__fanout

    @fanout.setter
    def fanout(self, val):
        self.__fanout = val

    def __str__(self):
        parent = '[' +','.join(map(lambda x: x.name, self.prev_nodes)) + ']'
        child = '[%s]' % (','.join(map(lambda x: x.name, self.sub_nodes)))
        return "name:%s, type:%s, in:%f, out:%f, parent:%s, child:%s,fanout:%f"\
               % (self.name,self.type, self.in_stream, self.out_stream, parent,
                  child, self.__fanout)

    def insert_sub_node(self, node):
        self._sub_nodes.append(node)

    def insert_prev_node(self, node):
        self._prev_nodes.append(node)

    def remove_sub_node(self, node):
        assert len(self.sub_nodes) != 0, "remove node through empty list"
        self._sub_nodes.remove(node)

    def remove_prev_node(self, node):
        assert len(self.prev_nodes) != 0, "remove node through empty list"
        self._prev_nodes.remove(node)

    def flow_data(self):
        """calculate the data flow of this current node"""
        self._in_stream = self._cal_in_stream(self._in_stream)
        self._out_stream = self.__fanout * float(self._in_stream)

    def _cal_in_stream(self, input_stream = None):
        if input_stream is None:
            if self.sub_nodes is None:
                raise Exception("Prev node is none for non-source node")
            else:
                input_stream = reduce(lambda r, x: r + x.out_stream,
                                      self.sub_nodes, 0)
        return input_stream

    def __is_validate_type(self):
        return self.type in type(self).VALID_TYPES

    def __valid_type(self):
        if not self.__is_validate_type():
            valid_type_str = ",".join(self.VALID_TYPES)
            raise Exception("Node type is not valid, valid types: %s, node type"
                            ": %s" % (valid_type_str, self.type))


class DAG(object):
    def __init__(self, root = None):
        self._root = root
        self._top_nodes = None
        self._leaves = None

    def str_helper(self):
        '''Helper function to work around the multiple inheritance limitation
        in python. For more, read:
        http://stackoverflow.com/questions/3277367/how-does-pythons-super-work
        -with-multiple-inheritance'''
        strs = map(lambda x: str(x), self.top_nodes)
        return '\n'.join(strs)

    def __str__(self):
        return self.str_helper()

    def __len__(self):
        return len(self.top_nodes)

    @property
    def top_nodes(self):
        if self._top_nodes is None:
            self._top_nodes = self.top_sort(self._root)
        return self._top_nodes

    @property
    def root(self):
        return self._root

    @property
    def leaves(self):
        if self._leaves is None:
            self._leaves = self._get_leaves()
        return self._get_leaves()

    def update(self):
        self._leaves = None
        self._top_nodes = None

    def _get_leaves(self):
        return filter(lambda x: len(x.sub_nodes) == 0, self.top_nodes)

    def build(self):
        # calculate the data streams
        for node in reversed(self.top_nodes):
            node.flow_data()


    def link_node(self, parent, child):
        if child not in parent.sub_nodes:
            parent.insert_sub_node(child)
        if parent not in child.prev_nodes:
            child.insert_prev_node(parent)

    def unlink_node(self, parent, child):
        if child in parent.sub_nodes:
            parent.remove_sub_node(child)
        if parent in child.prev_nodes:
            child.remove_prev_node(parent)

    def top_sort(self, root = None):
        """Get a list of nodes that is topology sorted."""
        if root is None:
            root = self._root
        result_list = []
        check_set = set()
        self.__top_sort_helper(result_list, check_set, self._root)
        return result_list[::-1]

    def __top_sort_helper(self, result_list, check_set, node):
        if node not in check_set:
            for sub_node in node.sub_nodes:
                self.__top_sort_helper(result_list, check_set, sub_node)
            check_set.add(node)
            result_list.append(node)

    def recursive_dispatcher(self, node, func, *args, **kwargs):
        if node is None:
            node = self._root
        if node is not self._root:
            func(node, *args, **kwargs)
        for sub_node in node.sub_nodes:
            self.recursive_dispatcher(sub_node, func, *args, **kwargs)

    def recursive_from_root(self, func, *args, **kwargs):
        func(self._root, *args, **kwargs)
        for node in self._root.sub_nodes:
            self.recursive_dispatcher(node, func, *args, **kwargs)

    def post_order_recursive_dispatcher(self, node, func, *args, **kwargs):
        for sub_node in node.sub_nodes:
            self.post_order_recursive_dispatcher(sub_node, func)
        # end for loop
        func(node, *args, **kwargs)