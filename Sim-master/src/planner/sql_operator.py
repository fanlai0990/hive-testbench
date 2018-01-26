''' Query operators label/type for global usage under this module
'''
# Helper operator
ROOT = "ROOT"
# sql operator types
# Hard Barrier, or a 'hard' synchronization barrier which cannot be split into
# combine + reduce
JOIN = "JOIN"
UNION = "UNION"
SINK = "SINK"
# Soft Barrier
# Special Soft Barrier
AGGREGATE = "AGGREGATE"
#   Stream Barrier
UNIQUE = "UNIQUE"
# Non-Barrier
SOURCE = "SOURCE"
#   Stream Non-barrier
FILTER = "FILTER" # represent both select and having and where for simplicity
LIMIT = "LIMIT"
# simplified physical plan node
MAP = "MAP"
COMBINE = "COMBINE"
REDUCE = "REDUCE"