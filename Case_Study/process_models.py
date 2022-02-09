import pm4py

def apply_alpha_miner_algorithm(log):
    print('Apply Alpha Miner:', len(log))
    ## Import the alpha_miner algorithm
    from pm4py.algo.discovery.alpha import algorithm as alpha_miner
    net, inital_marking, final_marking = alpha_miner.apply(log)

    ## Import the petrinet visualizer object
    from pm4py.visualization.petri_net import visualizer as pn_visualizer
    # Visualize
    gviz = pn_visualizer.apply(net, inital_marking, final_marking, log=log)
    pn_visualizer.view(gviz)

    calculate_quality_metrics(log, net, inital_marking, final_marking)

def apply_inductive_miner_algorithm(log):
    print('Apply Inductive Miner:')
    from pm4py.algo.discovery.inductive import algorithm as inductive_miner
    # Discover process tree using inductive miner
    tree = inductive_miner.apply_tree(log)

    from pm4py.visualization.process_tree import visualizer as pt_visualizer
    gviz = pt_visualizer.apply(tree, parameters={pt_visualizer.Variants.WO_DECORATION.value.Parameters.FORMAT: "png"})
    pt_visualizer.view(gviz)

    #generate petri-net
    from pm4py.objects.conversion.process_tree import converter as pt_converter
    net, inital_marking, final_marking = pt_converter.apply(tree, variant=pt_converter.Variants.TO_PETRI_NET)

    ## Import the petrinet visualizer object
    from pm4py.visualization.petrinet import visualizer as pn_visualizer
    # Visualize
    gviz = pn_visualizer.apply(net, inital_marking, final_marking, log=log)
    pn_visualizer.view(gviz)

    calculate_quality_metrics(log, net, inital_marking, final_marking)

    return tree

def apply_heuristic_miner_algorithm(log):
    print('Apply Heuristic Miner:')
    # get petri net
    from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
    net, im, fm = heuristics_miner.apply(log, parameters={
        heuristics_miner.Variants.CLASSIC.value.Parameters.DEPENDENCY_THRESH: 0.5})

    from pm4py.visualization.petri_net import visualizer as pn_visualizer
    gviz = pn_visualizer.apply(net, im, fm)
    pn_visualizer.view(gviz)

    calculate_quality_metrics(log, net, im, fm)

def calculate_quality_metrics(log, net, im, fm):
    from pm4py.algo.evaluation.replay_fitness import algorithm as replay_fitness_evaluator
    from pm4py.algo.evaluation.precision import algorithm as precision_evaluator
    from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator
    from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator

    fitness = replay_fitness_evaluator.apply(log, net, im, fm, variant=replay_fitness_evaluator.Variants.TOKEN_BASED)
    print('Fitness: ', fitness)

    prec = precision_evaluator.apply(log, net, im, fm, variant=precision_evaluator.Variants.ETCONFORMANCE_TOKEN)
    print('Precision: ', prec)

    gen = generalization_evaluator.apply(log, net, im, fm)
    print('Generalization: ', gen)

    simp = simplicity_evaluator.apply(net)
    print('Simplicity: ', simp)

def generate_bpmn(tree):
    from pm4py.objects.conversion.process_tree import converter
    bpmn_graph = converter.apply(tree, variant=converter.Variants.TO_BPMN)
    pm4py.write_bpmn(bpmn_graph, "process_model.bpmn", enable_layout=True)

def convert_pn_to_tree(net, im, fm):
    from pm4py.objects.conversion.wf_net import converter as wf_net_converter
    tree = wf_net_converter.apply(net, im, fm)
    return tree

def generate_process_models(event_log):

    import os
    os.environ["PATH"] += os.pathsep + 'C:\\Program Files\\Graphviz\\bin'

    apply_alpha_miner_algorithm(event_log)
    apply_heuristic_miner_algorithm(event_log)
    tree = apply_inductive_miner_algorithm(event_log)
    generate_bpmn(tree)
