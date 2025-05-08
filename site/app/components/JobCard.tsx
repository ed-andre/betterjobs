import { Job } from "~/services/api";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "~/components/ui/card";
import { Badge } from "~/components/ui/badge";

interface JobCardProps {
  job: Job;
  onClick: () => void;
}

export function JobCard({ job, onClick }: JobCardProps) {
  const handleCardClick = (e: React.MouseEvent) => {
    e.preventDefault();
    onClick();
  };

  return (
    <Card
      className="h-full flex flex-col hover:shadow-lg transition-shadow cursor-pointer"
      onClick={handleCardClick}
      role="button"
      tabIndex={0}
      style={{ cursor: 'pointer' }}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <CardHeader>
        <div className="flex justify-between items-start">
          <CardTitle className="text-xl text-blue-600 mb-2">{job.job_title}</CardTitle>
          <Badge variant="outline">{job.platform}</Badge>
        </div>
        <CardDescription className="text-base font-medium">
          {job.company_name || 'Company'}
        </CardDescription>

        <div className="text-sm text-gray-500 mt-1">
          {job.location || 'Location not specified'}
        </div>
      </CardHeader>
      <CardContent className="flex-grow">
        <p className="text-gray-700 text-sm line-clamp-3">
          {job.job_description ?
            job.job_description.substring(0, 150) + (job.job_description.length > 150 ? '...' : '')
            : 'No description available.'}
        </p>
      </CardContent>
      <CardFooter className="flex justify-between items-center pt-2 text-xs text-gray-500">
        <span>
          {job.department &&
            <Badge variant="secondary" className="mr-2">{job.department}</Badge>
          }
        </span>
        <span>
          {job.date_posted ?
            `Posted: ${new Date(job.date_posted).toLocaleDateString()}` :
            'Posted date unknown'}
        </span>
      </CardFooter>
    </Card>
  );
}