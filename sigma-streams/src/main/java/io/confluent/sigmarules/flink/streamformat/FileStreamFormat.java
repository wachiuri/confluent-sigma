package io.confluent.sigmarules.flink.streamformat;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;
import java.io.IOException;

public class FileStreamFormat extends SimpleStreamFormat<String> {
    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        return new Reader(stream);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @PublicEvolving
    public static final class Reader implements StreamFormat.Reader<String> {

        private final FSDataInputStream stream;

        public Reader(FSDataInputStream stream) {
            this.stream = stream;
        }

        @Nullable
        @Override
        public String read() throws IOException {
            return new String(stream.readAllBytes());
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }
}
