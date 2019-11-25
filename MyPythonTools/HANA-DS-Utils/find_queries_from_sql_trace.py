import argparse
import re
from datetime import datetime
import os
import heapq
import multiprocessing as mp
import logging


logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(message)s')

class Confs:
    def __init__(self):
        self.args = self.parse_command_line()

    def parse_command_line(self):
        parser = argparse.ArgumentParser(description="""Parse query execution information from SQL Traces.""")
        parser.add_argument('--file', required=True, type=str, nargs='+', help='SQL trace files location, when setting multiple files, use AA.trc BB.trc')
        parser.add_argument('--out-file', required=False, default='out', type=str, help='Output file location except extension (default out)')
        parser.add_argument('--out-format', required=False, default='txt', type=str, help='Output file format, choose one [txt(*), xlsx]')
        parser.add_argument('--sql-string', required=False, default='', type=str, help='Filter SQLs that doesn''t contain this word')
        parser.add_argument('--sql-length', required=False, default=50, type=int, help='Length of the SQL (default 100), 0 means that the SQL should not be sliced')
        parser.add_argument('--arg-length', required=False, default=50, type=int, help='Length of the SQL arguments (default 100), 0 means that the arguments should not be sliced')
        parser.add_argument('--sort-by', required=False, default='none', type=str, help='Set sort key for the output, choose one [none(*), begin_dt, end_dt, duration]')
        args = parser.parse_args()

        return args


def parse_queries(file, conf):
    QUERY_MAX_LENGTH = conf.sql_length
    ARGUMENT_MAX_LENGTH = conf.arg_length
    SQL_STRING = conf.sql_string
    SORT_BY = conf.sort_by

    content_pattern = re.compile(
        '(?P<begin># begin (?P<type>CallableStatement_execute|PreparedStatement_execute|PreparedStatement_executeUpdate)[^\n]+)\n(?P<connection># con info[^\n]+)\n(?P<query>cursor_.*?)\n(?P<end># end (?P=type) [^\n]+)',
        flags=re.DOTALL
    )
    begin_pattern = re.compile(
        '# begin .* \(thread (?P<thread_id>\d+), con-id (?P<connection_id>\d+)\) at (?P<begin_dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})'
    )
    end_pattern = re.compile(
        '# end .* \(thread (?P<thread_id>\d+), con-id (?P<connection_id>\d+)\) at (?P<end_dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})'
    )
    connection_pattern = re.compile(
        '# con info \[con-id (?P<connection_id>\d+), tx-id (?P<transaction_id>\d+), cl-pid (?P<client_process_id>\d+), cl-ip (?P<client_ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}), user: (?P<user>\w+), schema: (?P<schema>\w+)\]'
    )
    query_pattern = re.compile(
        "cursor_\d+_c\d+.execute(?:many)?\('''(?P<query>.*?)'''(, (?P<arguments>.*))?\)",
        flags=re.DOTALL
    )

    logging.info('{} reading start'.format(os.path.basename(file)))

    contents = open(file, 'rt+').read()
    executions = []

    logging.info('{} reading finish'.format(os.path.basename(file)))

    for content_no, content in enumerate(re.finditer(content_pattern, contents)):
        logging.info('{} {} detected'.format(os.path.basename(file), content_no))

        try:
            content = content.groupdict()
            begin = re.fullmatch(begin_pattern, content['begin']).groupdict()
            end = re.fullmatch(end_pattern, content['end']).groupdict()
            connection = re.fullmatch(connection_pattern, content['connection']).groupdict()
            query = re.match(query_pattern, content['query']).groupdict()

            logging.debug('{} {} begin {}'.format(os.path.basename(file), content_no, begin))
            logging.debug('{} {} end {}'.format(os.path.basename(file), content_no, begin))
            logging.debug('{} {} connection {}'.format(os.path.basename(file), content_no, begin))
            logging.debug('{} {} query {}'.format(os.path.basename(file), content_no, begin))

            if begin['connection_id'] != end['connection_id'] \
                    or begin['connection_id'] != connection['connection_id'] \
                    or begin['thread_id'] != end['thread_id']:
                logging.warning(
                    '{} {} connection id({}, {}, {}) and thread id({}, {}) are mismatch, skip this content'.format(
                        os.path.basename(file), content_no, begin['connection_id'], connection['connection_id'], end['connection_id'], begin['thread_id'], end['thread_id']
                    )
                )
                logging.warning('{} {} begin {}'.format(os.path.basename(file), content_no, begin))
                continue

            if SQL_STRING.lower() not in query['query'].lower():
                logging.info('{} {} query doesn''t have the string {}, skip this content'.format(os.path.basename(file), content_no, SQL_STRING))
                continue

            duration = (
                    datetime.strptime(end['end_dt'], '%Y-%m-%d %H:%M:%S.%f') - datetime.strptime(begin['begin_dt'], '%Y-%m-%d %H:%M:%S.%f')
            ).total_seconds()

            try:
                query['query'] = query['query'].strip().replace('\n', ' ')
                if QUERY_MAX_LENGTH and len(query['query']) > QUERY_MAX_LENGTH:
                    query['query'] = query['query'][:QUERY_MAX_LENGTH] + ' ..'
                query['arguments'] = query['arguments'].strip().replace('\n', ' ')
                if ARGUMENT_MAX_LENGTH and len(query['arguments']) > ARGUMENT_MAX_LENGTH:
                    query['arguments'] = query['arguments'][:ARGUMENT_MAX_LENGTH] + ' ..'
            except AttributeError:
                pass
            except TypeError:
                pass

            execution = [
                begin['begin_dt'],
                end['end_dt'],
                '{:.3f}'.format(duration),
                os.path.basename(file),
                connection['connection_id'],
                begin['thread_id'],
                connection['transaction_id'],
                connection['client_ip'],
                connection['user'],
                connection['schema'],
                query['query'],
                query['arguments']
            ]

            logging.debug('{} {} execution {}'.format(os.path.basename(file), content_no, execution))

            executions.append(execution)

            logging.info('{} {} execution at {} has parsed'.format(os.path.basename(file), content_no, execution[0]))
        except AttributeError:
            logging.warning('{} {} some errors occurred, skip this content'.format(os.path.basename(file), content_no))
            pass

    logging.info('{} file parsing finish'.format(os.path.basename(file)))
    logging.info('{} file sorting start'.format(os.path.basename(file)))

    if SORT_BY == 'begin_dt':
        executions.sort(key=lambda x: x[0])
    elif SORT_BY == 'end_dt':
        executions.sort(key=lambda x: x[1])
    elif SORT_BY == 'duration':
        executions.sort(key=lambda x: x[2])

    logging.info('{} file sorting finsih'.format(os.path.basename(file)))

    return executions


def write_executions(executions, conf):
    FORMAT = conf.out_format
    FILE = conf.out_file
    headers = ['BEGIN_DT', 'END_DT', 'DURATION (s)', 'FILE', 'CONNECTION_ID', 'THREAD_ID', 'TRANSACTION_ID', 'CLIENT_IP', 'USER', 'SCHEMA', 'QUERY', 'ARGUMENTS']

    if not FILE.endswith('.{}'.format(FORMAT)):
        FILE += '.{}'.format(FORMAT)

    logging.info('Output file {}'.format(FILE))

    if FORMAT == 'txt':
        import tabulate

        with open(FILE, 'wt+', encoding='utf-8') as f:
            f.write(tabulate.tabulate(executions, headers=headers, tablefmt='orgtbl'))
    elif FORMAT == 'xlsx':
        import xlsxwriter

        workbook = xlsxwriter.Workbook(FILE)
        worksheet = workbook.add_worksheet('sql_trace_queries')

        for col, item in enumerate(headers):
            worksheet.write(0, col, item)

        for row, execution in enumerate(executions, start=1):
            for col, item in enumerate(execution):
                worksheet.write(row, col, item)

        workbook.close()


def main():
    conf = Confs().args

    logging.info('Arguments {}'.format(conf))

    executions = []
    pool_results = []

    with mp.Pool(processes=min(len(conf.file), mp.cpu_count())) as pool:
        for file in conf.file:
            pool_results.append(pool.apply_async(parse_queries, (file, conf)))
            logging.info('File {} start to parse SQLs'.format(os.path.basename(file)))

        if conf.sort_by == 'none':
            for pool_result in pool_results:
                executions.extend(pool_result.get())
        else:
            for pool_result in pool_results:
                executions.append(pool_result.get())

        logging.info('All file''s data has parsed')
        logging.info('Merge sort of sorted data start')

    if conf.sort_by == 'begin_dt':
        executions = heapq.merge(*executions, key=lambda x: x[0])
    elif conf.sort_by == 'end_dt':
        executions = heapq.merge(*executions, key=lambda x: x[1])
    elif conf.sort_by == 'duration':
        executions = heapq.merge(*executions, key=lambda x: x[2])

    logging.info('Merge sort of sorted data finish')
    logging.info('Write data to file start')

    write_executions(executions, conf)

    logging.info('Write data to file finish')


if __name__ == '__main__':
    main()
