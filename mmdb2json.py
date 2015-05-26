#!/usr/bin/python
# This file is part of MMDB2JSON.
#
# Copyright(c) 2015 Simone Margaritelli
# evilsocket@gmail.com
# http://www.evilsocket.net
#
# This file may be licensed under the terms of of the
# GNU General Public License Version 2 (the ``GPL'').
#
# Software distributed under the License is distributed
# on an ``AS IS'' basis, WITHOUT WARRANTY OF ANY KIND, either
# express or implied. See the GPL for the specific language
# governing rights and limitations.
#
# You should have received a copy of the GPL along with this
# program. If not, go to http://www.gnu.org/licenses/gpl.html
# or write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
import sys
import struct
import netaddr
import json

from maxminddb.decoder import Decoder
from maxminddb.reader import Metadata
from maxminddb.compat import byte_from_int

class MMDB:
    METADATA_BEGIN_MARKER = "\xAB\xCD\xEFMaxMind.com"
    DATA_SECTION_SEPARATOR_SIZE = 16

    def __init__(self, filename):
        with open( filename, 'rb' ) as input:
            self.data = input.read()

            try:
                pos = self.data.rindex( MMDB.METADATA_BEGIN_MARKER )
                pos += len(MMDB.METADATA_BEGIN_MARKER)
            except:
                raise Exception( "Could not find metadata marker, invalid database." )

            metadata_decoder = Decoder(self.data, pos)
            self.metadata = metadata_decoder.decode(pos)[0]
            self.metadata = Metadata( **self.metadata )
            self.decoder = Decoder(self.data, self.metadata.search_tree_size + MMDB.DATA_SECTION_SEPARATOR_SIZE)

    def dump(self, callback):
        root = 0
        ip = 0
        depth = 1
        max_depth = 32 if self.metadata.ip_version == 4 else 128

        self._recurse( root, ip, depth, max_depth, callback )


    def _recurse(self, root, ip, depth, max_depth, callback):
        children = ( self._read_node(root, 0), \
                     self._read_node(root, 1) )

        for bit, child in enumerate(children):
            # We ignore empty branches of the search tree
            if child == self.metadata.node_count:
                continue

            if bit == 1:
                ip |= ( 1 << ( max_depth - depth ) )

            if child < self.metadata.node_count:
                self._recurse( child, ip, depth + 1, max_depth, callback )

            else:
                callback( ip, depth, self._resolve_data_pointer(child) )

    def _resolve_data_pointer(self, pointer):
        resolved = pointer - self.metadata.node_count + \
            self.metadata.search_tree_size

        if resolved > len(self.data):
            raise Exception("The MaxMind DB file's search tree is corrupt")

        (data, _) = self.decoder.decode(resolved)
        return data

    def _read_node(self, node_number, index):
        base_offset = node_number * self.metadata.node_byte_size
        record_size = self.metadata.record_size

        if record_size == 24:
            offset = base_offset + index * 3
            node_bytes = b'\x00' + self.data[offset:offset + 3]

        elif record_size == 28:
            (middle,) = struct.unpack(b'!B', self.data[base_offset + 3:base_offset + 4])
            if index:
                middle &= 0x0F
            else:
                middle = (0xF0 & middle) >> 4

            offset = base_offset + index * 4
            node_bytes = byte_from_int(middle) + self.data[offset:offset + 3]

        elif record_size == 32:
            offset = base_offset + index * 4
            node_bytes = self.data[offset:offset + 4]

        else:
            raise Exception('Unknown record size: {0}'.format(record_size))

        return struct.unpack(b'!I', node_bytes)[0]

if len(sys.argv) < 3:
    print "Usage: %s <input.mmdb> <output.json>" % sys.argv[0]
    quit()

fout = open( sys.argv[2], 'wt' )
fout.write( "[" )

def callback( ip, depth, data ):
    global fout

    o= {
        "net": str(netaddr.IPAddress(ip)),
        "bits" : depth,
        "data" : data
    }

    fout.write( json.dumps(o) + "," )

db = MMDB( sys.argv[1] )

print "@ Dumping database ..."

db.dump( callback )

fout.write( "]" )
fout.close()

print "@ Fixing json ..."

# remove the last comma from the file, i know this sucks
# but it's the only way without loading every-fucking-thing
# in RAM.
with open( sys.argv[2], 'rt' ) as f:
    data = f.read()

data = data.replace( "},]", "}]" )

with open( sys.argv[2], 'w+t' ) as f:
    f.write(data)
