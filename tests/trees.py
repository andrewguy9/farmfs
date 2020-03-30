from farmfs.fs import sep, ROOT, Path, LINK, DIR
from itertools import permutations, combinations, chain, product
from collections import defaultdict

def permute_deep(options):
    options = [permutations(options, pick) for pick in range(1,1+len(options))]
    return list(chain.from_iterable(options))

def combine_deep(options):
    options = [combinations(options, pick) for pick in range(1,1+len(options))]
    return list(chain.from_iterable(options))

def orphans(paths):
    accum = set()
    for path in paths:
        accum.add(path)
        parent = path.parent()
        if path != ROOT and parent not in accum:
            yield path

def has_orphans(paths):
    return len(list(orphans(paths))) > 0

def no_orphans(paths):
    return not has_orphans(paths)

def tree_shapes(names):
    paths = generate_paths(names)
    shapes = combine_deep(paths)
    return filter(no_orphans, shapes)

def generate_trees(segments, csums):
    shapes = tree_shapes(segments)
    trees = list(chain(*list(map(lambda tree: makeTreeOptions(tree, csums), shapes))))
    return trees

def permuteOptions(seq, options):
    optionSeq = [options[item] for item in seq]
    return product(*optionSeq)

def makeTreeOptions(tree, csums):
    return permuteOptions(tree, makeTreeOptionDict(tree, csums))

#TODO we are generating Path here, but keySnap needs to be tolerant of that. It wants BaseString
def generate_paths(names):
    return list(map(Path, ["/"]+list(map(lambda segs: "/"+"/".join(segs), permute_deep(names)))))

def makeTreeOptionDict(paths, csums):
    ppaths = parents(paths)
    assert ROOT in ppaths
    lpaths = leaves(paths)
    dirPaths = ppaths.union(lpaths)
    linkPaths = lpaths
    dirCombos = makeDirectoryPermutations(dirPaths)
    linkCombos = makeLinkPermutations(linkPaths, csums)
    combined = {path: dirCombos[path] + linkCombos[path] for path in paths}
    return combined

def parents(paths):
    ppaths = set([ROOT]).union(map(lambda p: p.parent(), paths))
    return ppaths

def leaves(paths):
    ppaths = parents(paths)
    lpaths = set(paths).difference(ppaths)
    return lpaths

def makeLinkPermutations(paths, csum_options):
    path_csum = product(paths, csum_options)
    links = {path:
            list(map(lambda csum: makeLink(path, csum), csum_options))
            for path in paths}
    return defaultdict(list, links)

def makeDirectoryPermutations(paths):
    dirs = {path: [makeDir(path)] for path in paths}
    return defaultdict(list, dirs)

def makeDir(path):
    return {"path": path, "type": DIR}

def makeLink(path, csum):
    return {"path": path, "csum": csum, "type": LINK}

