import { createClient } from '@supabase/supabase-js';


export interface Job {
  id: number;
  job_id: string;
  company_id: string;
  company_name: string | null;
  platform: string;
  job_title: string;
  job_description: string | null;
  job_url: string;
  location: string | null;
  department: string | null;
  date_posted: string | null;
  date_retrieved: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  if (typeof window === 'undefined') {
    console.warn("Supabase URL or Anon Key is not set. API calls will fail.");
    // throw new Error("SupABASE_URL and SUPABASE_ANON_KEY must be set in environment variables on the server.");
  } else {
    console.warn("Supabase URL or Anon Key is not set. Ensure they are configured in your environment.");
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

interface FetchJobsOptions {
  limit?: number;
  page?: number;
  platform?: string;
}

export async function getJobs(options: FetchJobsOptions = {}): Promise<Job[]> {
  console.log("Attempting to fetch jobs from Supabase...");
  console.log("Supabase URL:", SUPABASE_URL ? "Set" : "Not Set");
  console.log("Supabase Anon Key:", SUPABASE_ANON_KEY ? "Set" : "Not Set");

  if (!supabase || !SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error("Supabase client not initialized. Returning empty array.");
    return [];
  }

  // Default options
  const limit = options.limit || 2000;
  const page = options.page || 0;
  const platform = options.platform;

  try {
    console.log(`Fetching jobs (page ${page}, limit ${limit})${platform ? ` for platform ${platform}` : ''}...`);

    let query = supabase
      .from('Job')
      .select(`
        id,
        job_id,
        company_id,
        company_name,
        platform,
        job_title,
        job_description,
        job_url,
        location,
        department,
        date_posted,
        date_retrieved,
        is_active,
        created_at,
        updated_at
      `)
      .eq('is_active', true);

    if (platform) {
      query = query.eq('platform', platform);
    }

    const offset = page * limit;
    query = query
      .order('date_posted', { ascending: false, nullsFirst: false })
      .order('date_retrieved', { ascending: false })
      .range(offset, offset + limit - 1);

    const { data, error, count } = await query;

    if (error) {
      console.error('Error fetching jobs from Supabase:', error);
      throw error;
    }

    console.log(`Successfully fetched ${data?.length || 0} jobs (offset ${offset})`);

    const processedData = (data || []).map(job => ({
      ...job,
      job_description: job.job_description && job.job_description.trim() !== "" ? job.job_description : null
    }));

    return processedData;
  } catch (error) {
    console.error('An unexpected error occurred while fetching jobs:', error);
    return [];
  }
}

export async function getAllJobs(): Promise<Job[]> {
  console.log("getAllJobs called");

  try {
    console.log("Fetching all jobs with direct query");
    const { data, error } = await supabase
      .from('Job')
      .select(`
        id,
        job_id,
        company_id,
        company_name,
        platform,
        job_title,
        job_description,
        job_url,
        location,
        department,
        date_posted,
        date_retrieved,
        is_active,
        created_at,
        updated_at
      `)
      .eq('is_active', true)
      .order('date_posted', { ascending: false, nullsFirst: false })
      .order('date_retrieved', { ascending: false })
      .limit(2000);

    if (error) {
      console.error("Error fetching jobs:", error);
      throw error;
    }

    console.log(`Total jobs fetched: ${data?.length || 0}`);

    const processedData = (data || []).map(job => ({
      ...job,
      job_description: job.job_description && job.job_description.trim() !== "" ? job.job_description : null
    }));

    return processedData;
  } catch (error) {
    console.error("Error in getAllJobs:", error);
    throw error;
  }
}

