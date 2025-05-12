import { createClient } from '@supabase/supabase-js';

/**
 * Job data structure from Supabase
 */
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
//   work_type: string | null;
//   compensation: string | null;
}

// Supabase connection configuration
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

/**
 * Options for fetching job listings
 */
interface FetchJobsOptions {
  limit?: number;
  page?: number;
  platform?: string;
}

/**
 * Fetches paginated job listings from Supabase with optional filtering
 */
export async function getJobs(options: FetchJobsOptions = {}): Promise<Job[]> {
  if (!supabase || !SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error("Supabase client not initialized. Check environment configuration.");
    return [];
  }

  // Default options
  const limit = options.limit || 2000;
  const page = options.page || 0;
  const platform = options.platform;
  const offset = page * limit;

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

    query = query
      .order('date_posted', { ascending: false, nullsFirst: false })
      .order('date_retrieved', { ascending: false })
      .range(offset, offset + limit - 1);

    const { data, error } = await query;

    if (error) {
      console.error('Error fetching jobs from Supabase:', error);
      throw error;
    }

    console.log(`Successfully fetched ${data?.length || 0} jobs`);

    return processJobData(data);
  } catch (error) {
    console.error('Error while fetching jobs:', error);
    return [];
  }
}

/**
 * Fetches all active job listings from Supabase, sorted by newest first
 */
export async function getAllJobs(): Promise<Job[]> {
  if (!supabase || !SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error("Supabase client not initialized. Check environment configuration.");
    return [];
  }

  try {
    console.log("Fetching all active jobs...");
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
      .limit(10000);

    if (error) {
      console.error("Error fetching jobs:", error);
      throw error;
    }

    console.log(`Total jobs fetched: ${data?.length || 0}`);

    return processJobData(data);
  } catch (error) {
    console.error("Error in getAllJobs:", error);
    throw error;
  }
}

/**
 * Process and sanitize job data from Supabase
 */
function processJobData(data: Job[] | null): Job[] {
  return (data || []).map(job => ({
    ...job,
    job_description: job.job_description && job.job_description.trim() !== "" ? job.job_description : null
  }));
}

