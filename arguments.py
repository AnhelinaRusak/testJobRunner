from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--queue', help='Queue to listen')
parser.add_argument('--machine', help='Name of the machine')
parser.add_argument('--gpu', help='Id of the GPU')
parser.add_argument('--repository', help='Path to the CV repository')
args = parser.parse_args()
