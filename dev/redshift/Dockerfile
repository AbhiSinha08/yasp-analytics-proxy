FROM deburky/redshift-local:latest

# Add YASP's official-driver checks and application data; leave Oblako unchanged.
RUN pip3 install --no-cache-dir --target /yasp/driver redshift-connector==2.2.0
COPY dev/redshift/qualify.py dev/redshift/seed.sql /yasp/
