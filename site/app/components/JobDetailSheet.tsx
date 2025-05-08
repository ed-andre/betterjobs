import { Job } from "~/services/api";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetClose } from "~/components/ui/sheet";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";

interface JobDetailSheetProps {
  job: Job | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function JobDetailSheet({ job, open, onOpenChange }: JobDetailSheetProps) {
  if (!job) return null;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-xl overflow-y-auto">
        <SheetHeader className="mb-6">
          <SheetTitle className="text-xl font-bold text-blue-700">{job.job_title}</SheetTitle>
          <div className="flex flex-wrap gap-2 mt-2">
            <Badge variant="outline">{job.platform}</Badge>
            {job.department &&
              <Badge variant="secondary">{job.department}</Badge>
            }
          </div>
          <SheetDescription className="text-base font-medium mt-2">
            {job.company_name || 'Company'} • {job.location || 'Location not specified'}
          </SheetDescription>
        </SheetHeader>

        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-2">Job Description</h3>
          <div className="prose max-w-none">
            {job.job_description ? (
              <div
                dangerouslySetInnerHTML={{
                  __html: job.job_description.replace(/\n/g, '<br />')
                }}
              />
            ) : (
              <p>No description available.</p>
            )}
          </div>
        </div>

        <div className="flex flex-col space-y-2 mt-6">
          <div className="text-sm text-gray-500">
            {job.date_posted ?
              `Posted: ${new Date(job.date_posted).toLocaleDateString()}` :
              'Posted date unknown'}
          </div>
          <div className="text-sm text-gray-500">
            ID: {job.job_id}
          </div>
        </div>

        <div className="mt-8 flex justify-between">
          <SheetClose asChild>
            <Button variant="outline">Close</Button>
          </SheetClose>
          <Button
            className="bg-blue-600 hover:bg-blue-700"
            onClick={() => window.open(job.job_url, '_blank')}
          >
            Apply Now
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}