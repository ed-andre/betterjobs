import type { Route } from "./+types/home";
import { useLoaderData, useNavigation } from "react-router";
import { getAllJobs } from "~/services/api";
import type { Job } from "~/services/api";
import { useState, useMemo, useEffect } from "react";

// Import UI components
import { Input } from "~/components/ui/input";
import { JobCard } from "~/components/JobCard";
import { JobDetailSheet } from "~/components/JobDetailSheet";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "BetterJobs - Find Your Next Opportunity" },
    { name: "description", content: "Browse available job opportunities all in one place." },
  ];
}

export async function loader() {
  // Use getAllJobs to get jobs across all platforms
  console.time('fetchAllJobs');
  const jobs = await getAllJobs();
  console.timeEnd('fetchAllJobs');
  console.log(`Total jobs fetched for UI: ${jobs.length}`);

  return new Response(JSON.stringify({ jobs }), {
    headers: { "Content-Type": "application/json" },
  });
}

export default function Home() {
  const { jobs } = useLoaderData() as { jobs: Job[] };
  const navigation = useNavigation();
  const isLoading = navigation.state === "loading";

  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [platformFilter, setPlatformFilter] = useState<string>("all");
  const [visibleCount, setVisibleCount] = useState(30); // Initially show 30 jobs

  // Load more jobs when user scrolls near the bottom
  const loadMore = () => {
    setVisibleCount(prev => prev + 30);
  };

  // Add scroll detection to load more jobs
  useEffect(() => {
    const handleScroll = () => {
      if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 500) {
        loadMore();
      }
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  // Reset visible count when filters change
  useEffect(() => {
    setVisibleCount(30);
  }, [searchQuery, platformFilter]);

  // Log job information to help debug
  useEffect(() => {
    if (jobs && jobs.length > 0) {
      console.log("Total number of jobs loaded:", jobs.length);

      // Count by platform
      const platformCounts: Record<string, number> = {};
      jobs.forEach(job => {
        const platform = job.platform || 'unknown';
        platformCounts[platform] = (platformCounts[platform] || 0) + 1;
      });

      console.log("Jobs by platform:", platformCounts);

      // Check for description presence
      const withDescription = jobs.filter(job => job.job_description && job.job_description.trim() !== "").length;
      console.log(`Jobs with descriptions: ${withDescription} / ${jobs.length}`);
    }
  }, [jobs]);

  // Get unique platforms for filter dropdown
  const platforms = useMemo(() => {
    if (!jobs || jobs.length === 0) return ["all"];

    const platformSet = new Set<string>();
    jobs.forEach(job => {
      if (job.platform) {
        platformSet.add(job.platform);
      }
    });

    console.log("Detected platforms:", Array.from(platformSet));
    return ["all", ...Array.from(platformSet)];
  }, [jobs]);

  // Filter jobs based on search query and platform filter
  const filteredJobs = useMemo(() => {
    if (!jobs) return [];

    return jobs.filter(job => {
      // Apply platform filter
      if (platformFilter !== "all" && job.platform !== platformFilter) {
        return false;
      }

      // Apply search query filter (case insensitive)
      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        return (
          (job.job_title && job.job_title.toLowerCase().includes(query)) ||
          (job.company_name && job.company_name.toLowerCase().includes(query)) ||
          (job.location && job.location.toLowerCase().includes(query)) ||
          (job.job_description && job.job_description.toLowerCase().includes(query))
        );
      }

      return true;
    });
  }, [jobs, searchQuery, platformFilter]);

  // Get only the visible slice of jobs
  const visibleJobs = useMemo(() => {
    return filteredJobs.slice(0, visibleCount);
  }, [filteredJobs, visibleCount]);

  return (
    <div className="container mx-auto py-8 px-4">
      <div className="flex flex-col items-center mb-8">
        <h1 className="text-3xl font-bold mb-4 text-center">Find Your Next Great Opportunity</h1>
        <p className="text-gray-600 text-center max-w-2xl mb-6">
          Browse through hundreds of job listings from top companies across various platforms, all in one place.
        </p>

        {/* Search and filter controls */}
        <div className="w-full max-w-4xl flex flex-col md:flex-row gap-3 mb-8">
          <div className="flex-grow">
            <Input
              type="text"
              placeholder="Search jobs, companies, or locations..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full"
              disabled={isLoading}
            />
          </div>
          <div className="w-full md:w-48">
            <select
              value={platformFilter}
              onChange={(e) => setPlatformFilter(e.target.value)}
              className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm"
              disabled={isLoading}
            >
              {platforms.map(platform => (
                <option key={platform} value={platform}>
                  {platform === "all" ? "All Platforms" : platform}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {isLoading ? (
        <div className="flex flex-col items-center justify-center py-12">
          <div className="w-16 h-16 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
          <p className="mt-4 text-gray-600">Loading job listings...</p>
        </div>
      ) : filteredJobs.length === 0 ? (
        <div className="text-center p-8 border rounded-lg bg-gray-50">
          <p className="text-gray-600">No job openings match your search. Try adjusting your filters.</p>
        </div>
      ) : (
        <>
          <div className="mb-4 text-sm text-gray-500">
            Showing {Math.min(visibleCount, filteredJobs.length)} of {filteredJobs.length} {filteredJobs.length === 1 ? 'job' : 'jobs'}
            {platformFilter !== "all" && ` from ${platformFilter}`}
            {searchQuery && ` matching "${searchQuery}"`}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {visibleJobs.map((job) => (
              <JobCard
                key={job.id}
                job={job}
                onClick={() => setSelectedJob(job)}
              />
            ))}
          </div>

          {visibleCount < filteredJobs.length && (
            <div className="flex justify-center my-8">
              <button
                onClick={loadMore}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
              >
                Load More Jobs
              </button>
            </div>
          )}

          <JobDetailSheet
            job={selectedJob}
            open={!!selectedJob}
            onOpenChange={(open) => !open && setSelectedJob(null)}
          />
        </>
      )}
    </div>
  );
}
