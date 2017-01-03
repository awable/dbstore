-- MySQL dump 10.13  Distrib 5.5.35, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: edgestore
-- ------------------------------------------------------
-- Server version	5.5.35-0ubuntu0.12.04.2-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `colo`
--

DROP TABLE IF EXISTS `colo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `colo` (
  `colo` int(11) unsigned NOT NULL DEFAULT '0',
  `counter` int(11) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`colo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `edgedata`
--

DROP TABLE IF EXISTS `edgedata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `edgedata` (
  `edgetype` int(11) unsigned NOT NULL DEFAULT '0',
  `gid1` bigint(20) unsigned NOT NULL DEFAULT '0',
  `gid2` bigint(20) unsigned NOT NULL DEFAULT '0',
  `revision` int(11) unsigned NOT NULL DEFAULT '0',
  `encoding` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `data` blob,
  PRIMARY KEY (`edgetype`,`gid1`,`gid2`),
  UNIQUE KEY `revision` (`edgetype`,`gid1`,`revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `edgeindex`
--

DROP TABLE IF EXISTS `edgeindex`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `edgeindex` (
  `indextype` int(11) unsigned NOT NULL DEFAULT '0',
  `indexvalue` varbinary(767) NOT NULL DEFAULT '',
  `gid1` bigint(20) unsigned NOT NULL DEFAULT '0',
  `revision` int(11) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`indextype`,`indexvalue`,`gid1`,`revision`),
  KEY `edge` (`indextype`,`gid1`,`revision`) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `edgemeta`
--

DROP TABLE IF EXISTS `edgemeta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `edgemeta` (
  `edgetype` int(11) unsigned NOT NULL DEFAULT '0',
  `gid1` bigint(20) unsigned NOT NULL DEFAULT '0',
  `count` int(11) unsigned NOT NULL DEFAULT '0',
  `revision` int(11) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`edgetype`,`gid1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2014-07-24  9:13:07
