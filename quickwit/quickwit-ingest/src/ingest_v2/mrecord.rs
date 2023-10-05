// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use bytes::{Buf, Bytes};
use prost::Message;
use quickwit_proto::ingest::{m_record_header, MRecordHeader};

pub(super) enum MRecordPayload {
    Doc(Bytes),
    Commit,
}

pub(super) struct MRecord {
    header: MRecordHeader,
    payload: MRecordPayload,
}

impl MRecord {
    pub fn new_doc(doc: Bytes) -> Self {
        Self {
            header: MRecordHeader::new_doc(),
            payload: MRecordPayload::Doc(doc),
        }
    }

    pub fn new_commit() -> Self {
        Self {
            header: MRecordHeader::new_commit(),
            payload: MRecordPayload::Commit,
        }
    }

    pub fn encode(&self) -> impl Buf {
        // TODO: this is going to allocate for each record. No bueno.
        let header_buf = Bytes::from(self.header.encode_length_delimited_to_vec());
        let payload_buf = match &self.payload {
            MRecordPayload::Doc(doc) => doc.clone(),
            MRecordPayload::Commit => Bytes::new(),
        };
        header_buf.chain(payload_buf)
    }

    pub fn decode(mut buf: impl Buf) -> Self {
        let header = MRecordHeader::decode_length_delimited(&mut buf).unwrap();

        match header.header {
            Some(m_record_header::Header::DocHeader(_)) => {
                let doc = buf.copy_to_bytes(buf.remaining());
                Self {
                    header,
                    payload: MRecordPayload::Doc(doc),
                }
            }
            Some(m_record_header::Header::CommitHeader(_)) => Self {
                header,
                payload: MRecordPayload::Commit,
            },
            None => panic!("MRecordHeader is missing"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mrecord_doc_roundtrip() {
        let record = MRecord::new_doc(Bytes::from_static(b"hello"));
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record);

        assert_eq!(record.header, decoded_record.header);
        assert!(matches!(record.payload, MRecordPayload::Doc(doc) if doc == "hello"));
    }

    #[test]
    fn test_mrecord_commit_roundtrip() {
        let record = MRecord::new_commit();
        let encoded_record = record.encode();
        let decoded_record = MRecord::decode(encoded_record);

        assert_eq!(record.header, decoded_record.header);
        assert!(matches!(record.payload, MRecordPayload::Commit));
    }
}
