import argparse
from graphviz import Graph
import os
import re
import logging
import multiprocessing as mp


class Confs:
    def __init__(self):
        self.args = self.parse_command_line()

    def parse_command_line(self):
        parser = argparse.ArgumentParser(description="""Parse SqlOptStep trace and draw graphs.""")
        parser.add_argument('--file', required=True, type=str, help="SqlOptStep trace file location")
        parser.add_argument('--sql-string', required=False, default='', type=str, help='Filter SQLs that doesn''t contain this word')
        parser.add_argument('--format', required=False, default='png', type=str, help='Output graph format [png(*), pdf, jpg]')
        parser.add_argument('--node-width', required=False, default=50, type=int, help='Length of the node in graph (default 50)')
        parser.add_argument('--parallel', required=False, default=4, type=int, help='Number of processes to work (default 4)')
        args = parser.parse_args()

        return args


def draw_plan_graph(plan, file_name=None, render_format='png', node_max_width=50):
    NODE_MAX_WIDTH = node_max_width

    content = plan.strip().split('\n')
    title = content.pop(0).replace('=', '').strip()
    changed_operator_ids = []

    while re.search('(Created|Deleted|Parent-updated|Contents-updated) Operator:', content[-1]):
        changed_operator_ids.extend(re.findall('(?:c|d|pu|cu)@(\d+)', content.pop()))

    logging.debug('Changed operator {} is found'.format(changed_operator_ids))

    graph = Graph(
        name=title,
        format=render_format,
        filename=file_name if file_name else '{}.{}'.format(title, 'gv'),
        graph_attr={
            'label': title,
            'labelloc': 'top',
            'labeljust': 'left',
            'fontname': 'Consolas',
            'fontsize': '14',
        },
        node_attr={
            'fontname': 'Consolas',
            'fontsize': '12',
            'shape': 'box',
            'style': 'filled',
            'fillcolor': 'cornsilk1'
        }
    )

    logging.debug('Graph {} is created'.format(graph))

    parent_operators = []

    for line in content:
        logging.debug('Plan line: {}'.format(line))

        if not line.strip().startswith('#'):
            continue

        depth = int(line.index('#') / 2)

        try:
            operator_id = re.search('\(opId:(\d+)\)', line).group(1)
            operator = re.search('# (.*?) \(opId:', line).group(1)
        except AttributeError:
            continue

        if not operator.isupper():
            continue

        logging.debug('depth: {}, operator_id: {}, operator: {}'.format(depth, operator_id, operator))

        node_attrs = {}
        descriptions = []

        if operator_id in changed_operator_ids:
            node_attrs['peripheries'] = '2'

        if 'JOIN' in operator:
            node_attrs['fillcolor'] = 'lightskyblue1'
            node_attrs['shape'] = 'ellipse'
        elif operator.startswith('TABLE'):
            node_attrs['fillcolor'] = 'darkseagreen2'

        if operator == 'PROJECT':
            try:
                description = 'project: ' + re.search('\((\(\d+, \d+\)(, )?)+\)', line).group(0)[1:-1]
                descriptions.append(description if len(description) <= NODE_MAX_WIDTH else '{} {}'.format(description[:NODE_MAX_WIDTH], ' ..'))
            except Exception:
                pass
        elif 'JOIN' in operator:
            try:
                description = 'pred: ' + re.search('PRED: (.*?) result', line).group(1).strip()
                descriptions.append(description if len(description) <= NODE_MAX_WIDTH else '{} {}'.format(description[:NODE_MAX_WIDTH], ' ..'))
            except Exception:
                pass

        if 'FILTER:' in line:
            try:
                description = 'filter: ' + re.search('FILTER: (.+?) (?:\[|TABLE|--|result|  )', line).group(1).strip()
                descriptions.append(description if len(description) <= NODE_MAX_WIDTH else '{} {}'.format(description[:NODE_MAX_WIDTH], ' ..'))
            except Exception:
                pass

        for no, description in enumerate(descriptions):
            descriptions[no] = description.replace('<', '&lt;').replace('>', '&gt;')

        graph.node(
            name=operator_id,
            label='<[opId:{}] {}{}>'.format(
                operator_id,
                operator,
                '' if not descriptions else '<font point-size="9"><br/> <br/>' + '<br/>'.join(descriptions) + '</font>'
            ),
            **node_attrs
        )

        while parent_operators and parent_operators[-1]['depth'] >= depth:
            parent_operators.pop()

        if depth != 0:
            graph.edge(parent_operators[-1]['operator_id'], operator_id)
            logging.debug('parent_operator_id: {}'.format(parent_operators[-1]['operator_id']))

        parent_operators.append({'operator_id': operator_id, 'depth': depth})

    return graph


def draw_jobs_for_parallel_process(plan, file_name, render_format, node_max_width, sub_path):
    graph = draw_plan_graph(plan, file_name, render_format, node_max_width)
    graph.render(directory=sub_path, cleanup=True)

    return True

def draw_plan_graphs_in_file(file, sql_string='', render_format='png', node_max_width=50, parallel=4):
    trace_pattern = re.compile('\[-?\d+\]{-?\d+}\[-?\d+(?:/-?\d+)\] (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) (?:d|i|w|c|e|f) SqlOptStep .*?(?=\n\[-?\d+\]{-?\d+}|$)', flags=re.DOTALL)
    plan_compile_pattern = re.compile('\[Query Compile\].*?(?=(\n\[Query|$))', flags=re.DOTALL)
    plan_optimizer_pattern = re.compile('\[Query Optimizer\].*?(?=(?:\n\[Query Optimizer\]|\n\[Estimated Memory Consumption\]|$))', flags=re.DOTALL)

    path = os.path.dirname(os.path.abspath(file))
    with open(file, 'rt+', encoding='utf-8') as f:
        traces = f.read().strip()

    logging.info('Start to search file')
    sub_path_no = 0

    for trace_no, trace in enumerate(re.finditer(trace_pattern, traces)):
        logging.info('SqlOptStep #{} is found at {} -----------------------------------'.format(trace_no, trace.group('timestamp')))
        trace = trace.group(0).strip()

        query = re.search(plan_compile_pattern, trace).group(0).split('\n', 1)[1]
        shorten_query = re.sub(' +', ' ', query.replace('\n', ' '))
        logging.info('SqlOptStep #{} query is detected [{}]'.format(
            trace_no,
            shorten_query if len(shorten_query) < 50 else shorten_query[:50] + ' ..')
        )

        if sql_string.upper() not in query.upper():
            logging.info('SqlOptStep #{} query doesn''t have the input word [{}], so skip'.format(trace_no, sql_string))
            continue

        plans = re.findall(plan_optimizer_pattern, trace)

        if not plans:
            logging.info('SqlOptStep #{} cannot find optimizing steps, so skip'.format(trace_no))
            continue

        sub_path = os.path.join(path, str(sub_path_no).rjust(2, '0'))
        sub_path_no += 1
        logging.info('SqlOptStep #{} >>> Sub Path {}'.format(trace_no, sub_path))

        if not os.path.exists(sub_path):
            os.mkdir(sub_path)

        with open(os.path.join(sub_path, 'SqlOptStep.trc'), 'wt+', encoding='utf-8') as f:
            f.write(re.sub('(^|\n)([^\[])', '\g<1>  \g<2>', trace))
            logging.info('SqlOptStep #{} SqlOptStep trace is written to a file'.format(trace_no))

        with open(os.path.join(sub_path, 'query.sql'), 'wt+', encoding='utf-8') as f:
            f.write(query)
            logging.info('SqlOptStep #{} query is written to a file'.format(trace_no))

        pool_results = []
        with mp.Pool(processes=parallel) as pool:
            for plan_no, plan in enumerate(plans):
                title = re.sub('\[|\]|=|\(|\)', '', plan.split('\n', 1)[0].lower()).strip().replace(' ', '_').replace('-', '_').replace(':', '_').replace('query_optimizer', 'qo')
                file_name = '{}_{}.gv'.format(str(plan_no).rjust(2, '0'), title)
                pool_results.append(pool.apply_async(draw_jobs_for_parallel_process, (plan, file_name, render_format, node_max_width, sub_path)))
                logging.info('SqlOptStep #{} plan #{} id detected and will be drawn to the file {}'.format(trace_no, plan_no, file_name))

            logging.info('SqlOptStep #{} plan #{} waiting for finishing all processes ..'.format(trace_no, plan_no))
            for pool_result in pool_results:
                pool_result.get()
            logging.info('SqlOptStep #{} plan #{} is finished'.format(trace_no, plan_no))

        logging.info('SqlOptStep #{} is finished --------------------------------'.format(trace_no))


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    conf = Confs().args
    logging.info('file: {}, sql-string: {}, format: {}, node-width: {}, parallel: {}'.format(conf.file, conf.sql_string, conf.format, conf.node_width, conf.parallel))

    try:
        draw_plan_graphs_in_file(conf.file, conf.sql_string, conf.format, conf.node_width, conf.parallel)
    except Exception as e:
        logging.exception('Error occurred while drawing graphs')
        raise e


if __name__ == '__main__':
    main()
