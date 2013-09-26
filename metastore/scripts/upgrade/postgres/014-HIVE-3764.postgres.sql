--
-- Table structure for VERSION
--
CREATE TABLE "VERSION" (
  "VER_ID" bigint,
  "SCHEMA_VERSION" character varying(127) NOT NULL,
  "COMMENT" character varying(255) NOT NULL,
  PRIMARY KEY ("VER_ID")
);
ALTER TABLE ONLY "VERSION" ADD CONSTRAINT "VERSION_pkey" PRIMARY KEY ("VER_ID");

INSERT INTO VERSION (VER_ID, SCHEMA_VERSION, COMMENT) VALUES (1, '', 'Initial value');
