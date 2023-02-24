CREATE TABLE public."askHistories" (id numeric NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "tokenId" text NOT NULL, value numeric NOT NULL, "timestamp" numeric NOT NULL, accepted numeric NOT NULL, "transactionHash" text NOT NULL, "lister" text, "chainName" text, "listingHash" text, "expiry" numeric);
ALTER TABLE public."askHistories" OWNER TO postgres;

CREATE TABLE public.asks (id text NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "tokenId" text NOT NULL, value numeric NOT NULL, "timestamp" numeric NOT NULL, "transactionHash" text NOT NULL, "lister" text, "chainName" text, "listingHash" text, "expiry" numeric);
ALTER TABLE public.asks OWNER TO postgres;

CREATE TABLE public.bids (id text NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "tokenId" text NOT NULL, value numeric NOT NULL, buyer text NOT NULL, "timestamp" numeric NOT NULL, "transactionHash" text NOT NULL, "expiry" numeric, "offerHash" text, "chainName" text, "seller" text);
ALTER TABLE public.bids OWNER TO postgres;

CREATE TABLE public.collections (id text NOT NULL, "volumeOverall" numeric DEFAULT 0, "floorPrice" numeric DEFAULT 0, "ceilingPrice" numeric DEFAULT 0, "chainName" text);
ALTER TABLE public.collections OWNER TO postgres;

CREATE TABLE public.fills (id text NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "tokenId" text NOT NULL, value numeric NOT NULL, "timestamp" numeric NOT NULL, buyer text NOT NULL, type text NOT NULL, "chainName" text, "tradeHash" text, "seller" text);
ALTER TABLE public.fills OWNER TO postgres;

CREATE TABLE public.holders (id text NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "currentOwner" text NOT NULL, "lastTransfer" numeric NOT NULL, "chainName" text);
ALTER TABLE public.holders OWNER TO postgres;

CREATE TABLE public.meta (name text NOT NULL, value text, "timestamp" numeric);
ALTER TABLE public.meta OWNER TO postgres;

CREATE TABLE public.tokens (id text NOT NULL, "collectionId" text NOT NULL, "tokenNumber" numeric NOT NULL, "currentAsk" numeric DEFAULT 0, "heighestBid" numeric DEFAULT 0, "lowestBid" numeric DEFAULT 0, "chainName" text);
ALTER TABLE public.tokens OWNER TO postgres;

CREATE TABLE public.transactions (id text NOT NULL, "blockNumber" numeric NOT NULL, "timestamp" numeric NOT NULL, "chainName" text);
ALTER TABLE public.transactions OWNER TO postgres;

CREATE TABLE public.activityHistories (eventId text NOT NULL, "userAddress" text NOT NULL, "activity" text NOT NULL, "chainName" text, "tokenAddress" text, "tokenNumber" text, "amount" numeric DEFAULT 0, "timestamp" numeric NOT NULL);
ALTER TABLE public.activityHistories OWNER TO postgres;

CREATE SEQUENCE public."askHistories_id_seq" AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER TABLE public."askHistories_id_seq" OWNER TO postgres;
ALTER SEQUENCE public."askHistories_id_seq" OWNED BY public."askHistories".id;
ALTER TABLE ONLY public."askHistories" ALTER COLUMN id SET DEFAULT nextval('public."askHistories_id_seq"'::regclass);

