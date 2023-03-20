SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE SCHEMA postgraphile_watch;
ALTER SCHEMA postgraphile_watch OWNER TO postgres;

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

CREATE FUNCTION postgraphile_watch.notify_watchers_ddl() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
begin
  perform pg_notify(
    'postgraphile_watch',
    json_build_object(
      'type',
      'ddl',
      'payload',
      (select json_agg(json_build_object('schema', schema_name, 'command', command_tag)) from pg_event_trigger_ddl_commands() as x)
    )::text
  );
end;
$$;
ALTER FUNCTION postgraphile_watch.notify_watchers_ddl() OWNER TO postgres;

CREATE FUNCTION postgraphile_watch.notify_watchers_drop() RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
begin
  perform pg_notify(
    'postgraphile_watch',
    json_build_object(
      'type',
      'drop',
      'payload',
      (select json_agg(distinct x.schema_name) from pg_event_trigger_dropped_objects() as x)
    )::text
  );
end;
$$;
ALTER FUNCTION postgraphile_watch.notify_watchers_drop() OWNER TO postgres;

SET default_tablespace = '';
SET default_with_oids = false;

CREATE TABLE public."askHistories" (
    "id" numeric NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "tokenId" text NOT NULL, value numeric NOT NULL, 
    "timestamp" numeric NOT NULL, 
    "accepted" numeric NOT NULL, 
    "transactionHash" text NOT NULL, 
    "lister" text, 
    "chainName" text, 
    "listingHash" text, 
    "expiry" numeric);
ALTER TABLE public."askHistories" OWNER TO postgres;

CREATE TABLE public.asks (
    "id" text NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "tokenId" text NOT NULL, 
    "value" numeric NOT NULL, 
    "timestamp" numeric NOT NULL, 
    "transactionHash" text NOT NULL, 
    "lister" text, 
    "chainName" text, 
    "listingHash" text, 
    "expiry" numeric);
ALTER TABLE public.asks OWNER TO postgres;

CREATE TABLE public.bids (
    "id" text NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "tokenId" text NOT NULL, 
    "value" numeric NOT NULL, 
    "buyer" text NOT NULL, 
    "timestamp" numeric NOT NULL, 
    "transactionHash" text NOT NULL, 
    "expiry" numeric, 
    "offerHash" text, 
    "chainName" text, 
    "seller" text);
ALTER TABLE public.bids OWNER TO postgres;

CREATE TABLE public.collections (
    "id" text NOT NULL, 
    "volumeOverall" numeric DEFAULT 0, 
    "floorPrice" numeric DEFAULT 0, 
    "ceilingPrice" numeric DEFAULT 0, 
    "chainName" text, 
    "royalty" numeric DEFAULT 0, 
    "tradingEnabled" boolean DEFAULT FALSE, 
    "collectionOwner" text, 
    "timestamp" numeric DEFAULT 0 NOT NULL, 
    "lastModifiedTxHash" text, 
    "isERC1155" boolean DEFAULT FALSE NOT NULL);
ALTER TABLE public.collections OWNER TO postgres;

CREATE TABLE public.fills (
    "id" text NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "tokenId" text NOT NULL, 
    "value" numeric NOT NULL, 
    "timestamp" numeric NOT NULL, 
    "buyer" text NOT NULL, 
    "type" text NOT NULL, 
    "chainName" text, 
    "tradeHash" text, 
    "seller" text, 
    "transactionHash" text,
    "isERC1155" boolean DEFAULT FALSE,
    "quantity" numeric DEFAULT 1);
ALTER TABLE public.fills OWNER TO postgres;

CREATE TABLE public.holders (
    "id" text NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "currentOwner" text NOT NULL, 
    "lastTransfer" numeric NOT NULL, 
    "chainName" text);
ALTER TABLE public.holders OWNER TO postgres;

CREATE TABLE public.meta (
    "name" text NOT NULL, 
    "value" text, 
    "timestamp" numeric);
ALTER TABLE public.meta OWNER TO postgres;

CREATE TABLE public.tokens (
    "id" text NOT NULL, 
    "collectionId" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "currentAsk" numeric DEFAULT 0, 
    "highestBid" numeric DEFAULT 0, 
    "lowestBid" numeric DEFAULT 0, 
    "chainName" text);
ALTER TABLE public.tokens OWNER TO postgres;

CREATE TABLE public.transactions (
    "id" text NOT NULL, 
    "blockNumber" numeric NOT NULL, 
    "timestamp" numeric NOT NULL, 
    "chainName" text);
ALTER TABLE public.transactions OWNER TO postgres;

CREATE TABLE public."activityHistories" (
    "eventId" text NOT NULL, 
    "userAddress" text NOT NULL, 
    "activity" text NOT NULL, 
    "chainName" text, 
    "tokenAddress" text, 
    "tokenNumber" text, 
    "amount" numeric DEFAULT 0, 
    "timestamp" numeric NOT NULL,
    "tradeHash" text,
    "transactionHash" text);
ALTER TABLE public."activityHistories" OWNER TO postgres;

CREATE SEQUENCE public."askHistories_id_seq" AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER TABLE public."askHistories_id_seq" OWNER TO postgres;
ALTER SEQUENCE public."askHistories_id_seq" OWNED BY public."askHistories".id;
ALTER TABLE ONLY public."askHistories" ALTER COLUMN id SET DEFAULT nextval('public."askHistories_id_seq"'::regclass);

-- 1155's
CREATE TYPE fungibleTradeStatus AS ENUM ('OPEN', 'PARTIAL', 'ACCEPTED', 'CANCELLED');
ALTER TYPE fungibleTradeStatus OWNER TO postgres;
CREATE TYPE fungibleTradeType AS ENUM ('BUY', 'SELL');
ALTER TYPE fungibleTradeStatus OWNER TO postgres;

CREATE TABLE public."fungibleTrades" (
    "tradeHash" text NOT NULL, 
    "contractAddress" text NOT NULL, 
    "tokenNumber" numeric NOT NULL, 
    "status" fungibleTradeStatus NOT NULL, 
    "tradeType" fungibleTradeType NOT NULL, 
    "allowPartials" boolean NOT NULL DEFAULT FALSE,
    "isEscrowed" boolean NOT NULL DEFAULT TRUE,
    "totalQuantity" numeric NOT NULL, 
    "remainingQuantity" numeric NOT NULL DEFAULT 0, 
    "pricePerUnit" numeric NOT NULL, 
    "openedTimestamp" numeric NOT NULL, 
    "lastUpdatedTimestamp" numeric DEFAULT 0,
    "chainName" text, 
    "maker" text NOT NULL, 
    "expiry" numeric DEFAULT 0);
ALTER TABLE public."fungibleTrades" OWNER TO postgres;

-- Best way I can think to do this for now - have one of these per chain. otherwise volumes get distorted
CREATE TABLE public."polygonTraders" (
    "userAddress" text NOT NULL,
    "saleCount" numeric DEFAULT 0,
    "saleVolume" numeric DEFAULT 0,
    "purchaseCount" numeric DEFAULT 0,
    "purchaseVolume" numeric DEFAULT 0,
    "offerCount" numeric DEFAULT 0,
    "offerVolume" numeric DEFAULT 0,
    "listingCount" numeric DEFAULT 0,
    "volume_and_count_score" numeric GENERATED ALWAYS AS ("saleVolume" + "purchaseVolume" + "offerVolume" + ("saleCount" * 1000000000000000000) + ("purchaseCount" * 1000000000000000000) + ("listingCount" * 1000000000000000000) + ("offerCount" * 1000000000000000000)) STORED,
    "pure_volume_score" numeric GENERATED ALWAYS AS ("saleVolume" + "purchaseVolume" + "offerVolume") STORED
);
ALTER TABLE public."polygonTraders" OWNER TO postgres;