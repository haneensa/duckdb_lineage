from abc import ABC, abstractmethod
from typing import Callable

class Op(ABC):
    def __init__(self, query_id: int, op: str, op_id: int, parent_join_cond: str, node: str, parent_join_type) -> None:
        self.single_op_table_name = f"LINEAGE_{query_id}_{op}_{op_id}"
        self.id = op_id
        self.is_root = parent_join_cond is None
        self.parent_join_cond = parent_join_cond
        self.is_agg_child = False
        
        self.extra = node['extra']
        self.join_type = ''
        self.parent_join_type = parent_join_type
        if "RIGHT" in self.extra:
            self.join_type = 'right'
        if len(self.parent_join_type) > 0:
            self.join_type = 'right'

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_from_string(self) -> str:
        pass

    @abstractmethod
    def get_child_join_conds(self) -> list:
        pass
    
    @abstractmethod
    def get_child_join_cond_type(self) -> str:
        pass

    @abstractmethod
    def get_out_index(self) -> str:
        pass

    @abstractmethod
    def get_in_index(self, cid) -> str:
        pass

class SingleOp(Op):
    def __init__(self, query_id: int, name: str, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, name, op_id, parent_join_cond, node, parent_join_type)

    @abstractmethod
    def get_name(self) -> str:
        pass

    def get_from_string(self) -> str:
        print("SingleOp", self.single_op_table_name, self.parent_join_type)
        if self.is_root:
            return self.single_op_table_name
        else:
            if self.is_agg_child:
                return "JOIN "  + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + "0"
            elif self.parent_join_type == 'right':
                return "LEFT JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"
            else:
                return "JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self) -> list:
        return [self.single_op_table_name + ".in_index"]
    
    def get_child_join_cond_type(self) -> str:
        return self.join_type

    def get_out_index(self) -> str:
        if self.is_agg_child:
            return "0 as out_index"
        return self.single_op_table_name + ".out_index"

    def get_in_index(self, cid) -> str:
        return self.single_op_table_name + ".in_index"

class Limit(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "LIMIT"

class ColScan(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "COLUMN_DATA_SCAN"



class StreamingLimit(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "STREAMING_LIMIT"


class Filter(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "FILTER"


class OrderBy(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "ORDER_BY"


class Projection(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "PROJECTION"


class TableScan(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "SEQ_SCAN"

class DelimScan(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str, delim_scan: dict=None) -> None:
        super().__init__(query_id, delim_scan['name'], op_id, parent_join_cond, delim_scan, parent_join_type)

    def get_name(self) -> str:
        return "DELIM_SCAN"


class GroupBy(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

class HashGroupBy(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "HASH_GROUP_BY"


class PerfectHashGroupBy(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "PERFECT_HASH_GROUP_BY"


class StandardJoin(Op):
    def __init__(self, query_id: int, name: str, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, name, op_id, parent_join_cond, node, parent_join_type)

    @abstractmethod
    def get_name(self) -> str:
        pass

    def get_child_join_conds(self) -> list:
        return [self.single_op_table_name + ".lhs_index", self.single_op_table_name + ".rhs_index"]
    
    def get_child_join_cond_type(self) -> str:
        return self.join_type

    def get_out_index(self) -> str:
        if self.is_agg_child:
            return "0 as out_index"
        return self.single_op_table_name + ".out_index"
    
    def get_in_index(self, cid) -> str:
        if cid == 0:
            return self.single_op_table_name + ".lhs_index"
        else:
            return self.single_op_table_name + ".rhs_index"

    def get_from_string(self) -> str:
        print("***** " , self.parent_join_type)
        if self.is_root:
            return  self.single_op_table_name
        elif self.is_agg_child:
            return "JOIN "  + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + "0"
        elif self.parent_join_type == 'right':
            return "LEFT JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"
        else:
            return "JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

class RightDelimJoin(Op):
    # children, join, distinct
    # distict: deduplicate the outer join
    # join: joins distinct with the original join
    # RightDelimJoin joins the left side in children[0] with the output of join
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)
        assert(len(node['children'])==3, 'right delim join children != 3')
        print('======= distinct ========')
        print(node['children'][0])
        distinct_op = node['children'][0]['name']
        distinct_opid = node['children'][0]['opid']
        self.distinct_op_table_name = f"LINEAGE_{query_id}_{distinct_op}_{distinct_opid}"
        print(self.distinct_op_table_name)

        print('======= join ========')
        # this would have dummy and delim scan
        # dummy scan reads from lhs. delim scan reads from distinct
        print(node['children'][1])
        delim_op = node['children'][1]['name']
        delim_opid = node['children'][1]['opid']
        self.single_op_table_name = f"LINEAGE_{query_id}_{delim_op}_{delim_opid}"
        print(self.single_op_table_name)

        print('======= delim join lhs child ======== ', len(node['children']))
        # we should recurse on this child to construct end o end query
        print(node['children'][2])
        lhs_op = node['children'][2]['name']
        lhs_opid = node['children'][2]['opid']
        # delim uses this in dummy scan?
        self.lhs_op_table_name = f"LINEAGE_{query_id}_{lhs_op}_{lhs_opid}"
        print(self.lhs_op_table_name)
        


    def get_name(self) -> str:
        return 'DELIM_JOIN'

    def get_child_join_conds(self) -> list:
        return [self.single_op_table_name + ".lhs_index", self.single_op_table_name + ".rhs_index"]
    
    def get_child_join_cond_type(self) -> str:
        return self.join_type

    def get_out_index(self) -> str:
        if self.is_agg_child:
            return "0 as out_index"
        return self.single_op_table_name + ".out_index"
    
    def get_in_index(self, cid) -> str:
        if cid == 0:
            return self.single_op_table_name + ".lhs_index"
        else:
            return self.single_op_table_name + ".rhs_index"

    def get_from_string(self) -> str:
        print("***** " , self.parent_join_type)
        if self.is_root:
            return  self.single_op_table_name
        elif self.is_agg_child:
            return "JOIN "  + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + "0"
        elif self.parent_join_type == 'right':
            return "LEFT JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"
        else:
            return "JOIN " + self.single_op_table_name \
                    + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

class HashJoin(StandardJoin):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "HASH_JOIN"
    

class BlockwiseNLJoin(StandardJoin):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "BLOCKWISE_NL_JOIN"


class PiecewiseMergeJoin(StandardJoin):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "PIECEWISE_MERGE_JOIN"


class CrossProduct(StandardJoin):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "CROSS_PRODUCT"


class NestedLoopJoin(StandardJoin):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "NESTED_LOOP_JOIN"

class UngroupedAggregate(SingleOp):
    def __init__(self, query_id: int, op_id: int, parent_join_cond: str, node: dict, parent_join_type: str) -> None:
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond, node, parent_join_type)

    def get_name(self) -> str:
        return "UNGROUPED_AGGREGATE"

    def get_from_string(self) -> str:
        return ""

class OperatorFactory():
    def get_op(self, op: str, op_id: int, query_id: int, parent_join_cond: str, node: dict, parent_join_type: str,
            delim_agg_distinct: dict=None, delim_join: dict=None) -> Op:
        op = op.strip()
        print('parent_join_type: ', parent_join_type, delim_agg_distinct)
        print(op, op_id, query_id, parent_join_cond, node, parent_join_type, delim_agg_distinct)
        if op == 'LIMIT':
            return Limit(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'STREAMING_LIMIT':
            return StreamingLimit(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'FILTER':
            return Filter(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'ORDER_BY':
            return OrderBy(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'PROJECTION':
            return Projection(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'SEQ_SCAN':
            return TableScan(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'HASH_GROUP_BY':
            return HashGroupBy(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'PERFECT_HASH_GROUP_BY':
            return PerfectHashGroupBy(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'HASH_JOIN':
            return HashJoin(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'BLOCKWISE_NL_JOIN':
            return BlockwiseNLJoin(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'PIECEWISE_MERGE_JOIN':
            return PiecewiseMergeJoin(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'CROSS_PRODUCT':
            return CrossProduct(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'NESTED_LOOP_JOIN':
            return NestedLoopJoin(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == 'UNGROUPED_AGGREGATE':
            return UngroupedAggregate(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == "RIGHT_DELIM_JOIN":
            return RightDelimJoin(query_id, op_id, parent_join_cond, node, parent_join_type)
        elif op == "DELIM_SCAN" or op == "DUMMY_SCAN":
            return DelimScan(query_id, op_id, parent_join_cond, node, parent_join_type, delim_agg_distinct)
        else:
            raise Exception('Found unhandled operator', op)
